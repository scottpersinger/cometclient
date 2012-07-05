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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.UrlEncoded;
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
    static String clientId;
    static String clientSecret;
    static ArrayList<StreamingEventEcho> _listeners = new ArrayList<StreamingEventEcho>();
    static Timer reconnectTimer = new Timer(true);
    static BlockingQueue<Object[]> publishQueue = new LinkedBlockingQueue<Object[]>();
	private static String queue = "sfpush.cmd";
    private static String dataQueue = "sfpush.data";
    
        // The path for the Streaming API endpoint
    private static final String DEFAULT_PUSH_ENDPOINT = "/cometd/24.0";
    private static ExecutorService _executor = null; 
    private static String dbUrl;
    
    static class SalesforceConfig {
    	String username;
    	String password;
    	String token;
    	
    }

    static class SalesforceOauth {
        String access_token;
        String endpoint;
        String refresh_token;
        String instance_url;        
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: StreamingEventEcho <dbURL> <rabbitURL> <clientId> <clientSecret>");
            System.err.println("dbUrl format: jdbc:mysql://host/database?user=user&password=password");
            System.err.println("rabbitUrl format: amqp://userName:password@hostName:portNumber/virtualHost");
            System.exit(-1);
        }
        setNoValidation();

        clientId = args[2];
        clientSecret = args[3];
        
        // Load the initial tenant list
        dbUrl = args[0];
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
    	// Bind for consuming commands, use same name for queue and routing key
    	channel.queueBind(queue, exchange, queue);
    	
    	// Create timer to reconnect to the Streaming API every hour or so
    	reconnectTimer.scheduleAtFixedRate(new TimerTask() {
    		public void run() {
    			// Reconnect to the streaming API
    			stopListeners();
    			runListeners();
    		}
    	}, 1000*60*55L, 1000*60*55L);
    	
        //runListeners();
        
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
							e.printStackTrace();
						}
	                	runListeners();
	                } else if (command.matches("restart:\\d+")) {
		                System.out.println("inside test 1");
	                	Matcher m = Pattern.compile("restart:(\\d+)").matcher(command);
	                	if (!m.matches()) {
	                		System.out.println("That didnt match");
	                		m = Pattern.compile(".*(\\d+).*").matcher(command);
	                	}
	                	String tid = m.group(1);
		                System.out.println("calling restart listener");
	                	//restartListener(Long.parseLong(tid));
	                }
	                // (process the message components here ...)
	                System.out.println("Acking Rabbit cmd " + command);
	                channel.basicAck(deliveryTag, false);
	                System.out.println("Ack done. Returning from handleDelivery");
	            }
        	}
        );
    	
        while (true) {
        	System.out.println(">> Waiting for messages to publish");
        	Object[] message = publishQueue.take();
        	System.out.println("<< Got message: " + message[1]);
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
    	    "mirror_tenant.id = mirror_keyvaluerecord.tenant_id " + 
    	    		"where mirror_keyvaluerecord.`key` = 'salesforce_config' " +
    	    		"OR mirror_keyvaluerecord.`key` = 'salesforce_oauth'")) {
    	    	ResultSet rs = stmt.getResultSet();
    	    	while (rs.next()) {
	    	    	long tenantId = rs.getLong("id");
	    	    	String tenantName = rs.getString("name");
	    	    	String json = rs.getString("payload_json");
	    	    	Gson gson = new Gson();
	    	    	// Try to parse as salesforce_config or salesforce_oauth
	    	    	SalesforceConfig sfConfig = null;
	    	    	SalesforceOauth sfOauth = null;
	    	    	try {
	    	    		sfConfig = gson.fromJson(json, SalesforceConfig.class);
	    	    		if (sfConfig.username == null) {
	    	    			sfConfig = null;
		    	    		sfOauth = gson.fromJson(json, SalesforceOauth.class);
	    	    		}
	    	    	} catch (Exception e) {
	    	    		sfOauth = gson.fromJson(json, SalesforceOauth.class);
	    	    	}
	    	    	// Now load the list of objects to listen to
	    	    	ArrayList<String> objectList = loadObjectList(tenantId, conn);
	    	    	System.out.println("Found tenant " + tenantId + " (" + tenantName + ")");
	    	    	_listeners.add(new StreamingEventEcho(tenantId, tenantName, sfConfig, sfOauth, objectList));
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

    public static ArrayList<String> loadObjectList(long tenantId, Connection conn) throws SQLException {
    	if (conn == null) {
    		conn = DriverManager.getConnection(dbUrl);
    	}
    	Statement objSt = conn.createStatement();
    	// This is a hack. We just jam 'streaming' into the LIKE for the payload to filter for objects
    	// that have streaming enabled. If someone stored "streaming":"false" then our logic would fail.
    	String sql = "select `key` from mirror_keyvaluerecord where tenant_id = " + tenantId 
    			+ " and `key` like 'salesforce.mappings.%'"
    			+ " and payload_json like '%streaming%'";
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
    	
    	return objectList;
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
    
    private static void restartListener(long tenant_id) {
    	for (int i = 0; i < _listeners.size(); i++) {
    		StreamingEventEcho listener = _listeners.get(i);
    		if (listener._tenantId == tenant_id) {
    			System.out.println("Stopping listener for tenant " + tenant_id);
    			listener.stop();
        		_listeners.remove(i);
        		// Reload list of objects to listen to
        		try {
					ArrayList<String> objectList = loadObjectList(tenant_id, null);
					System.out.println("Loaded new object list for tenant " + tenant_id + ": ");
					for (String s : objectList) {
						System.out.println("  " + s);
					}
					StreamingEventEcho newListener = new StreamingEventEcho(listener._tenantId, listener._tenantName, 
	    	    			listener._sfConfig, listener._sfOauth, objectList); 
	    	    	_listeners.add(newListener);
	    	    	_executor.execute(newListener);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        		break;
    		}
    	}
    }
    
    public long _tenantId;
    public String _tenantName;
    public SalesforceConfig _sfConfig;
    public SalesforceOauth _sfOauth;
    private List<String> _topics;
    private boolean _keepRunning;
    public String _sid;
    public boolean _connectFailed401;
    
    public StreamingEventEcho(long tenantId, String tenantName, SalesforceConfig sfConfig, 
    		SalesforceOauth sfOauth, List<String> topics) {
    	_tenantId = tenantId;
    	_tenantName = tenantName;
    	_sfConfig = sfConfig;
    	_sfOauth = sfOauth;
    	_topics = topics;
    }
    
	@Override
	public void run() {
		_keepRunning = true;
		try {
			while (_keepRunning) {
				// TODO Auto-generated method stub
				System.out.println("Streaming listener for tenant " + _tenantName + "(" + _tenantId + ")");
				System.out.println(_tenantId + ": Listening for topics: " + _topics.toString());
		
				BayeuxClient client = null;
				if (_sfConfig != null) {
					System.out.println(_tenantId + ": Connecting to Salesforce: " + _sfConfig.username + " / " + _sfConfig.password + ":" + _sfConfig.token);
					client = getClient(_sfConfig.username, _sfConfig.password + _sfConfig.token, null);
				} else if (_sfOauth != null){
					System.out.println(_tenantId + ": Connecting to Salesforce by Oauth: " + _sfOauth.access_token + " / " + _sfOauth.instance_url);
					client = getClient(null, null, _sfOauth);
				} else {
					throw new Exception("Error, no SalesforceConfig or SalesforceOauth available");
				}
				_connectFailed401 = false;
		        client.handshake();
		
		        System.out.println("Waiting for handshake");
		        try {
		        	waitForHandshake(client, 20 * 1000, 1000);
		        } catch (IllegalStateException e) {
		        	System.out.println("Got an Illegal state after handshake:" + e);
		        	client.abort();
		        	if (_sfOauth != null) {
		        		// Request a new access_token using our refresh_token
		        		JSONObject jsonResult = getNewAccessToken(_sfOauth);
		        		System.out.println("JSON response: " + jsonResult.toString());
		        		_sfOauth.access_token = jsonResult.getString("access_token");
		        		// FIXME: We should store the new access_token in the db
		        		// loop back and try to auth again
		        		continue;
		        	}
		        	return;
		        }
		
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
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			System.out.println(_tenantId + ": [ERROR] " + e.getMessage());
		}
	}

	public void stop() {
		_keepRunning =false;
	}
	
    private BayeuxClient getClient(String username, String password, SalesforceOauth sfOauth) throws Exception {
        HttpClient authClient = new HttpClient();
        authClient.start();

    	String instance_url;
        
        if (username != null && password != null) {
	    	// Use old style SOAP login that only needs username/password
	    	String[] pair = SoapLoginUtil.login(authClient, username, password);
			if (pair == null) {
				throw new RuntimeException("Salesforce authentication failed");
			}
	    	_sid = pair[0];
	    	instance_url = pair[1];
        } else {
        	_sid = sfOauth.access_token;
        	instance_url = sfOauth.instance_url;
        }
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
                exchange.addRequestHeader("Authorization", "OAuth " + StreamingEventEcho.this._sid);
            }
        };

        // Now set up the Bayeux client itself
        BayeuxClient client = new BayeuxClient(instance_url
                + DEFAULT_PUSH_ENDPOINT, transport) {
        	public void onFailure(Throwable xx, Message[] messages) {
        		System.out.println("BayeuxClient error: "+ xx.getMessage());
        		if (xx.getMessage().indexOf("response 401") >= 0) {
        			System.out.println("Detecting bad auth");
        			StreamingEventEcho.this._connectFailed401 = true;
        		}
        	}
        };

        return client;
    }

    private void waitForHandshake(BayeuxClient client,
            long timeoutInMilliseconds, long intervalInMilliseconds) {
        long start = System.currentTimeMillis();
        long end = start + timeoutInMilliseconds;
        while (System.currentTimeMillis() < end) {
        	if (_connectFailed401) {
        		throw new IllegalStateException("AUth failed");
        	}
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

    private JSONObject getNewAccessToken(SalesforceOauth sfOauth) throws Exception {
        HttpClient httpClient = new HttpClient();
        httpClient.start();

        String url = LOGIN_SERVER + "/services/oauth2/token";

        ContentExchange exchange = new ContentExchange();
        exchange.setMethod("POST");
        exchange.setURL(url);

        UrlEncoded params = new UrlEncoded();
        params.add("grant_type", "refresh_token");
        params.add("refresh_token", sfOauth.refresh_token);
        params.add("client_id", clientId);
        params.add("client_secret", clientSecret);
        params.add("format", "json");
        
        String message = "grant_type=refresh_token"
        		+ "&client_id=" + clientId
                + "&client_secret=" + clientSecret
                + "&refresh_token=" + sfOauth.refresh_token.replace("=", "%3D")
                + "&format=json";

        message = "client_secret=7176140918132069100&grant_type=refresh_token&client_id=3MVG9rFJvQRVOvk7dcW8zRXD4lv7ZdT638O2Ti7hrGGo2Oa7iFCoyabLryNhGv_j2x9yo0Ea6.VmqzUR6LDgD&refresh_token=5Aep861.EkZJuT7_lvUke.TdvF45yyzDcG.5kCM04ffwz31rauYNIMGGMWnsQJtiI8uGxRTh43L7Q%3D%3D&format=json";
        System.out.println("POSTING: " + message);
        
        //exchange.setRequestHeader("Content-Type",
        //        "application/x-www-form-urlencoded");
        exchange.setRequestContentSource(new ByteArrayInputStream(message
                .getBytes("UTF-8")));
        //exchange.setRequestContentType("application/x-www-form-urlencoded;charset=utf-8");

        httpClient.send(exchange);
        exchange.waitForDone();

        return new JSONObject(new JSONTokener(exchange.getResponseContent()));
    }
}
