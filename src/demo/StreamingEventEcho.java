package demo;

import java.io.ByteArrayInputStream;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

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

public class StreamingEventEcho {
    // Skips certificate validation in the case of logging proxies for debugging
    private static final boolean NO_VALIDATION = false;

    // The long poll duration
    private static final int TIMEOUT = 120 * 1000;

    // Get credentials etc from environment
    private static final String LOGIN_SERVER = "https://login.salesforce.com";
    private static String USERNAME = System.getenv("USERNAME");
    private static String PASSWORD = System.getenv("PASSWORD");

    private static final String CLIENT_ID = System.getenv("CLIENT_ID");
    private static final String CLIENT_SECRET = System.getenv("CLIENT_SECRET");

    // The path for the Streaming API endpoint
    private static final String DEFAULT_PUSH_ENDPOINT = "/cometd/24.0";

    public static void main(String[] args) throws Exception {
        BayeuxClient client = null;

        if (args.length < 3) {
            System.err.println("Usage: StreamingEventEcho <username> <password> <topic1> <topic2> ...");
            System.exit(-1);
        }

        USERNAME = args[0];
        PASSWORD = args[1];
        
        String topics[] = Arrays.copyOfRange(args, 2, args.length);

        System.out.println("Listening for topics: " + Arrays.asList(topics).toString());
        if (NO_VALIDATION) {
            setNoValidation();
        }

        client = getClient();
        client.handshake();

        System.out.println("Waiting for handshake");
        waitForHandshake(client, 60 * 1000, 1000);

        for (int i = 0; i < topics.length; i++) {
	        System.out.println("Subscribing to topic: " + topics[i]);
	        client.getChannel("/topic/" + topics[i]).subscribe(new MessageListener() {
	            @Override
	            public void onMessage(ClientSessionChannel channel, Message message) {
                	System.out.println("[EVENT] " + message.getJSON());
                	System.out.println("-------------------------------------------------------------------------");
                	System.out.println("-------------------------------------------------------------------------");
                	System.out.flush();
//	                try {
//	                    System.out.println("Received Message: "
//	                            + (new JSONObject(
//	                                    new JSONTokener(message.getJSON())))
//	                                    .toString(2));
//	                } catch (JSONException e) {
//	                    e.printStackTrace();
//	                }
	            }
	        });
        }
        System.out.println("Waiting for streamed data from Force.com...");
        while (true) {
            // This infinite loop is for demo only, to receive streamed events
            // on the specified topic from Salesforce.com
            Thread.sleep(TIMEOUT);
        }
    }

    private static BayeuxClient getClient() throws Exception {
        HttpClient authClient = new HttpClient();
        authClient.start();
        
    	// Use old style SOAP login that only needs username/password
    	String[] pair = SoapLoginUtil.login(authClient, USERNAME, PASSWORD);
		if (pair == null) {
			System.exit(1);
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

    private static void waitForHandshake(BayeuxClient client,
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

    private static JSONObject oauthLogin() throws Exception {
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

    }
}
