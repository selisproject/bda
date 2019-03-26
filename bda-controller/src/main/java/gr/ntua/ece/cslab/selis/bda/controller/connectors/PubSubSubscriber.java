package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import de.tu_dresden.selis.pubsub.*;

import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private static String certificateLocation;
    private static String SCNslug;

    private static volatile PubSubSubscription subscriptions = new PubSubSubscription();
    private static volatile boolean reloadSubscriptionsFlag = true;

    public PubSubSubscriber(String authHash, String hostname, int portNumber, String cert, String scn) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;
        this.certificateLocation = cert;
        this.SCNslug = scn;
    }

    public void reloadSubscriptions(PubSubSubscription subscriptions) {
        if (!subscriptions.getScnSlug().matches(this.SCNslug)){
            LOGGER.log(Level.SEVERE, "Asked to reload subscriptions of different SCN subscriber! This should never happen.");
            return;
        }
        this.subscriptions = subscriptions;
        this.reloadSubscriptionsFlag = true;
    }

    public String getAccessToken(String username, String password) {
        String POST_URL = "https://selis-gw.cslab.ece.ntua.gr:8443/auth/realms/selisrealm/protocol/openid-connect/token";

        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(POST_URL);

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("client_id", "pubsub_client"));
        params.add(new BasicNameValuePair("username", username));
        params.add(new BasicNameValuePair("password", password));
        params.add(new BasicNameValuePair("grant_type", "password"));
        params.add(new BasicNameValuePair("client_secret", "pubsub_secret"));

        try {
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
        }

        //Execute and get the response.
        HttpResponse response = null;
        try {
            response = httpclient.execute(httppost);
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }

        String results = null;
        try {
            results = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }

        JSONObject responseObject = new JSONObject(results);

        //System.out.println(responseObject);

        return (String) responseObject.get("access_token");
    }

    @Override
    public void run() {
        PubSub pubsub = null;

        while (reloadSubscriptionsFlag) {
            reloadSubscriptionsFlag = false;

            try {
                if (subscriptions.getPubSubHostname() != null)
                    this.hostname = subscriptions.getPubSubHostname();
                if (subscriptions.getPubSubPort() != null)
                    this.portNumber = subscriptions.getPubSubPort();
                pubsub = new PubSub(this.certificateLocation, this.hostname, this.portNumber);

                if (!(subscriptions.getSubscriptions().isEmpty())) {

                    for (Tuple messageTypeName : subscriptions.getSubscriptions()) {
                        String authHash = getAccessToken("selis_bda", "bda_secret");
                        Subscription subscription = new Subscription(authHash);

                        for (KeyValue rule: messageTypeName.getTuple())
                            subscription.add(new Rule(rule.getKey(), rule.getValue(), RuleType.EQ));

                        pubsub.subscribe(subscription, new Callback() {
                            @Override
                            public void onMessage(Message message) {
                                try {
                                    PubSubMessageHandler.handleMessage(message, SCNslug);
                                    LOGGER.log(Level.INFO,"PubSub message successfully inserted in the BDA.");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    LOGGER.log(Level.SEVERE,"Could not insert new PubSub message.");
                                }
                            }
                        });
                    }

                    LOGGER.log(Level.INFO,
                            String.format("SUCCESS: %s subscriber subscribed to %d message types",
                                    SCNslug, subscriptions.getSubscriptions().size()));
                }
                else
                    LOGGER.log(Level.INFO,
                            "{0} subscriber: No registered messages to subscribe to.", SCNslug);
            } catch (PubSubException ex) {
                LOGGER.log(Level.WARNING,
                           "Could not subscribe, got error: {0}",
                           ex.getMessage());
                pubsub.close();
            } catch (Exception e) {
                e.printStackTrace();
                pubsub.close();
            }

            while (true) {
                try {
                    Thread.sleep(300);

                    if (reloadSubscriptionsFlag) {
                        pubsub.close();
                        break;
                    }
                } catch (InterruptedException e) {
                    LOGGER.log(Level.WARNING,"{0} subscriber was interrupted.", SCNslug);
                    pubsub.close();
                    break;
                }
            }
        }
        pubsub.close();
        LOGGER.log(Level.INFO,"{0} subscriber finished.", SCNslug);
    }
}
