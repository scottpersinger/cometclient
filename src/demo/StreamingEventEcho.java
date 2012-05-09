package demo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.client.ClientSessionChannel.MessageListener;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.StringRpcServer;

public class StreamingEventEcho implements Runnable {
    // Skips certificate validation in the case of logging proxies for debugging
    private static final boolean NO_VALIDATION = false;

    // The long poll duration
    private static final int TIMEOUT = 120 * 1000;

    // Get credentials etc from environment
    public static final String LOGIN_SERVER = "https://login.salesforce.com";
    private static String dbuser;
    private static String dbpass;
    static String dbhost;
    static String dbname;
    static ArrayList<StreamingEventEcho> _listeners = new ArrayList<StreamingEventEcho>();
    static Timer reconnectTimer = new Timer(true);
    static BlockingQueue<Object[]> publishQueue = new LinkedBlockingQueue<Object[]>();
	private static String queue = "sfpush.cmd";
    private static String dataQueue = "sfpush.data";
    
        // The path for the Streaming API endpoint
    private static final String DEFAULT_PUSH_ENDPOINT = "/cometd/24.0";
    private static ExecutorService _executor = null; 

    static class SalesforceConfig {
    	String username;
    	String password;
    	String token;
    	
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StreamingEventEcho <dbURL> <rabbitURL>");
            System.err.println("dbUrl format: jdbc:mysql://host/database?user=user&password=password");
            System.err.println("rabbitUrl format: amqp://userName:password@hostName:portNumber/virtualHost");
            System.exit(-1);
        }
        setNoValidation();

        // Load the initial tenant list
        final String dbUrl = args[0];
    	loadTenants(dbUrl);

    	// Now connect to Rabbit MQ
    	ConnectionFactory connFactory = new ConnectionFactory();
    	System.out.println("Connecting to rabbit: "+ args[1]);
    	connFactory.setUri(args[1]);
    	
    	com.rabbitmq.client.Connection qconn = connFactory.newConnection();
    	final Channel channel = qconn.createChannel();
    	String exchange = "sfmirror";
    	channel.exchangeDeclare(exchange, "topic");
    	
    	System.out.println("Declaring queue: " + queue);
    	channel.queueDeclare(queue, true, true, false, null);
    	channel.queueDeclare(dataQueue, true, false, false, null);
    	System.out.println("Binding queue "+ queue + " to exchange " + exchange);
    	channel.queueBind(queue, exchange, queue);
    	
    	// Create timer to reconnect to the Streaming API every hour or so
    	reconnectTimer.scheduleAtFixedRate(new TimerTask() {
    		public void run() {
    			// Reconnect to the streaming API
    			stopListeners();
    			runListeners();
    		}
    	}, 1000*60*55L, 1000*60*55L);
    	
        runListeners();
        
        System.out.println("Consuming from queue: " + queue);
        channel.basicConsume(queue, true, 
        	new DefaultConsumer(channel) {
	        	@Override
	            public void handleDelivery(String consumerTag,
	                                       Envelope envelope,
	                                       AMQP.BasicProperties properties,
	                                       byte[] body)
	                throws IOException
	            {
	                String routingKey = envelope.getRoutingKey();
	                String contentType = properties.getContentType();
	                long deliveryTag = envelope.getDeliveryTag();
	                String command = new String(body);
	                System.out.println("Rabbit mq command: " + command);
	                if (command.equals("stop")) {
	                	System.out.println("Shutting down SF listeners...");
	                	stopListeners();
	                	channel.close();
	                	System.exit(0);
	                } else if (command.equals("reload")) {
	                	System.out.println("Reloading. Stopping listeners...");
	                	stopListeners();
	                	// Reload the Tenant list
	                	try {
							loadTenants(dbUrl);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	                	runListeners();
	                }
	                // (process the message components here ...)
	                channel.basicAck(deliveryTag, false);
	            }
        	}
        );
    	
        while (true) {
        	System.out.println("Waiting for messages to publish");
        	Object[] message = publishQueue.take();
        	System.out.println("Got message: " + message[1]);
        	String body = (String)message[1];
        	String msg = message[0] + ":" + body;
        	channel.basicPublish(exchange, "sfpush.data", null, msg.getBytes("UTF-8"));
        }
    }
    
    private static void loadTenants(String dbConn) throws SQLException {
    	_listeners = new ArrayList<StreamingEventEcho>();
    	
        // Connect to main db, load all tenants and start listening to their SF streams
    	try {
    		System.out.println("Connecting to: " + dbConn);
    	    Connection conn =
    	       DriverManager.getConnection(dbConn);

    	    // Do something with the Connection
    	    Statement stmt = conn.createStatement();
    	    // Load the set of Salesforce connection credentials per tenant
    	    if (stmt.execute("select mirror_tenant.id,name,payload_json from mirror_tenant inner join mirror_keyvaluerecord on " + 
    	    "mirror_tenant.id = mirror_keyvaluerecord.tenant_id where mirror_keyvaluerecord.`key` = 'salesforce_config'")) {
    	    	ResultSet rs = stmt.getResultSet();
    	    	while (rs.next()) {
	    	    	long tenantId = rs.getLong("id");
	    	    	String tenantName = rs.getString("name");
	    	    	String json = rs.getString("payload_json");
	    	    	Gson gson = new Gson();
	    	    	SalesforceConfig sfConfig = gson.fromJson(json, SalesforceConfig.class);
	    	    	// Now load the list of objects to listen to
	    	    	Statement objSt = conn.createStatement();
	    	    	String sql = "select `key` from mirror_keyvaluerecord where tenant_id = " + tenantId + " and `key` like 'salesforce.mappings.%'";
	    	    	ArrayList<String> objectList = new ArrayList<String>();
	    	    	if (objSt.execute(sql)) {
	    	    		ResultSet rs2 = objSt.getResultSet();
	    	    		while (rs2.next()) {
	    	    			// Grab object name off the end of the key
	    	    			// FIXME: We should record the topic name in the db and use that here
	    	    			objectList.add("cm." + rs2.getString(1).split("\\.")[2]);
	    	    		}
	    	    	}
	    	    	objSt.close();
	    	    	System.out.println("Found tenant " + tenantId + " (" + tenantName + ")");
	    	    	_listeners.add(new StreamingEventEcho(tenantId, tenantName, sfConfig, objectList));
    	    	}
    	    }
    	    

    	} catch (SQLException ex) {
    	    // handle any errors
    	    System.out.println("SQLException: " + ex.getMessage());
    	    System.out.println("SQLState: " + ex.getSQLState());
    	    System.out.println("VendorError: " + ex.getErrorCode());
    	    System.out.println("[FATAL ERROR] " + ex.getMessage());
    	    throw ex;
    	}    	
    }    	


    private static void runListeners() {
        _executor = Executors.newCachedThreadPool();
    	for (StreamingEventEcho listener : _listeners) {
    		_executor.execute(listener);
    	}
    }

    private static void stopListeners() {
    	for (StreamingEventEcho listener : _listeners) {
    		listener.stop();
    	}
    	try {
			if (!_executor.awaitTermination(10, TimeUnit.SECONDS)) {
				// force kill
				_executor.shutdownNow();
			}
			_executor = null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private long _tenantId;
    private String _tenantName;
    private SalesforceConfig _sfConfig;
    private List<String> _topics;
    private boolean _keepRunning;
    
    public StreamingEventEcho(long tenantId, String tenantName, SalesforceConfig sfConfig, List<String> topics) {
    	_tenantId = tenantId;
    	_tenantName = tenantName;
    	_sfConfig = sfConfig;
    	_topics = topics;
    }
    
	@Override
	public void run() {
		_keepRunning = true;
		
		try {
			// TODO Auto-generated method stub
			System.out.println("Streaming listener for tenant " + _tenantName + "(" + _tenantId + ")");
			System.out.println(_tenantId + ": Listening for topics: " + _topics.toString());
	
			System.out.println(_tenantId + ": Connecting to Salesforce: " + _sfConfig.username + " / " + _sfConfig.password + ":" + _sfConfig.token);
	        BayeuxClient client = getClient(_sfConfig.username, _sfConfig.password + _sfConfig.token);
	        client.handshake();
	
	        System.out.println("Waiting for handshake");
	        waitForHandshake(client, 60 * 1000, 1000);
	
	        for (String topic : _topics) {
		        System.out.println(_tenantId + ": Subscribing to topic: " + topic);
		        client.getChannel("/topic/" + topic).subscribe(new MessageListener() {
		            @Override
		            public void onMessage(ClientSessionChannel channel, Message message) {
	                	System.out.println(_tenantId + ": [EVENT] " + message.getJSON());
	                	Object[] payload = {_tenantId, message.getJSON()};
	                	publishQueue.add(payload);
		            }
		        });
	        }
	        System.out.println(_tenantId + ": READY for streamed data from Force.com...");
	        while (_keepRunning) {
	            // This infinite loop is for demo only, to receive streamed events
	            // on the specified topic from Salesforce.com
	            Thread.sleep(1000);
	        }
	        System.out.println(_tenantId + ": stopping");
	        for (String topic : _topics) {
		        client.getChannel("/topic/" + topic).unsubscribe();
	        }
	        client.disconnect();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			System.out.println(_tenantId + ": [ERROR] " + e.getMessage());
		}
	}

	public void stop() {
		_keepRunning =false;
	}
	
    private BayeuxClient getClient(String username, String password) throws Exception {
        HttpClient authClient = new HttpClient();
        authClient.start();
        
    	// Use old style SOAP login that only needs username/password
    	String[] pair = SoapLoginUtil.login(authClient, username, password);
		if (pair == null) {
			throw new RuntimeException("Salesforce authentication failed");
		}
    	final String sid = pair[0];
    	String instance_url = pair[1];
		
		/*
        // Authenticate via OAuth
        JSONObject response = oauthLogin();
        System.out.println("Login response: " + response.toString(2));
        if (!response.has("access_token")) {
            throw new Exception("OAuth failed: " + response.toString());
        }

        // Get what we need from the OAuth response
        final String sid = response.getString("access_token");
        String instance_url = response.getString("instance_url");
        */

        // Set up a Jetty HTTP client to use with CometD
        HttpClient httpClient = new HttpClient();
        httpClient.setConnectTimeout(TIMEOUT);
        httpClient.setTimeout(TIMEOUT);
        httpClient.start();
        
        Map<String, Object> options = new HashMap<String, Object>();
        options.put(ClientTransport.TIMEOUT_OPTION, TIMEOUT);
        
        // Adds the OAuth header in LongPollingTransport
        LongPollingTransport transport = new LongPollingTransport(
                options, httpClient) {
            @Override 
            protected void customize(ContentExchange exchange) {
                super.customize(exchange);
                exchange.addRequestHeader("Authorization", "OAuth " + sid);
            }
        };

        // Now set up the Bayeux client itself
        BayeuxClient client = new BayeuxClient(instance_url
                + DEFAULT_PUSH_ENDPOINT, transport);

        return client;
    }

    private void waitForHandshake(BayeuxClient client,
            long timeoutInMilliseconds, long intervalInMilliseconds) {
        long start = System.currentTimeMillis();
        long end = start + timeoutInMilliseconds;
        while (System.currentTimeMillis() < end) {
            if (client.isHandshook())
                return;
            try {
                Thread.sleep(intervalInMilliseconds);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalStateException("Client did not handshake with server");
    }

    public static void setNoValidation() throws Exception {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs,
                    String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs,
                    String authType) {
            }
        } };

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

        // Create all-trusting host name verifier
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };

        // Install the all-trusting host verifier
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    }

    /**
    private JSONObject oauthLogin() throws Exception {
        HttpClient httpClient = new HttpClient();
        httpClient.start();

        String url = LOGIN_SERVER + "/services/oauth2/token";

        ContentExchange exchange = new ContentExchange();
        exchange.setMethod("POST");
        exchange.setURL(url);

        String message = "grant_type=password&client_id=" + CLIENT_ID
                + "&client_secret=" + CLIENT_SECRET + "&username=" + USERNAME
                + "&password=" + PASSWORD;

        exchange.setRequestHeader("Content-Type",
                "application/x-www-form-urlencoded");
        exchange.setRequestContentSource(new ByteArrayInputStream(message
                .getBytes("UTF-8")));

        httpClient.send(exchange);
        exchange.waitForDone();

        return new JSONObject(new JSONTokener(exchange.getResponseContent()));

    }**/

}
