package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutEngineCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutableCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.Kpi;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;

import gr.ntua.ece.cslab.selis.bda.controller.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.controller.beans.MessageType;

import de.tu_dresden.selis.pubsub.*;
import de.tu_dresden.selis.pubsub.PubSubException;

import java.sql.SQLException;
import java.util.*;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    private final static int DEFAULT_VECTOR_SIZE = 10;

    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private List<String> messageTypeNames;

    private static volatile boolean reloadMessageTypesFlag = true;

    public PubSubSubscriber(String authHash, String hostname, int portNumber) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;
    }

    public static void reloadMessageTypes() {
        reloadMessageTypesFlag = true;
    }

    @Override
    public void run() {
        Vector<Subscription> subscriptions = new Vector<Subscription>(DEFAULT_VECTOR_SIZE);

        while (reloadMessageTypesFlag) {
            reloadMessageTypesFlag = false;
            subscriptions.clear();

            try (PubSub c = new PubSub(this.hostname, this.portNumber)) {
                messageTypeNames = MessageType.getActiveMessageTypeNames();

                for (String messageTypeName : messageTypeNames) {
                    Subscription subscription = new Subscription(this.authHash);

                    subscription.add(new Rule("message_type", messageTypeName, RuleType.EQ));

                    c.subscribe(subscription, new Callback() {
                        @Override
                        public void onMessage(Message message) {
                            LOGGER.log(Level.INFO,
                                       "Received Message: {0}",
                                       message.toString());
                            try {
                                handleMessage(message);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });

                    subscriptions.addElement(subscription);
                }

                LOGGER.log(Level.INFO, 
                           "SUCCESS: Subscribed to {0} message types", 
                           messageTypeNames.size());

                while (true) {
                    try {
                        Thread.sleep(300);

                        if (reloadMessageTypesFlag) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARNING,"Subscriber was interrupted.");
                        break;
                    }
                }
            } catch (PubSubException ex) {
                LOGGER.log(Level.WARNING, 
                           "Could not subscribe, got error: {0}", 
                           ex.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        LOGGER.log(Level.INFO,"Finishing");
    }

    private void handleMessage(Message message) throws Exception {
        // TODO: This somehow should distiguish between different messages
        //       and perform the corresponding actions.
        //
        //       Some tests about this wouldn't hurt us.

        if (true) {
            // Original code for message type: `SonaeStockLevels`.
            /*AnalyticsSystem mySystem = AnalyticsSystem.getInstance();
            ExecutEngineCatalog executEngineCatalog = ExecutEngineCatalog.getInstance();
            ExecutableCatalog executableCatalog = mySystem.getExecutableCatalog();
            KpiCatalog kpiCatalog = mySystem.getKpiCatalog();
            KpiFactory kpiFactory = mySystem.getKpiFactory();
            executEngineCatalog.addNewExecutEngine("spark", "spark-submit");
            List<String> argtypes = Arrays.asList();
            executableCatalog.addNewExecutable(argtypes, executEngineCatalog.getExecutEngine(0), "/home/ubuntu/selis/sonae2.py",
                    "This calculates an order proposal ");
            List<String> eng_arguments = Arrays.asList("--driver-class-path", "/home/ubuntu/selis/postgresql-42.2.1.jar", "--jars", "/home/ubuntu/selis/postgresql-42.2.1.jar");
            String description = "SONAE ORDER PROPOSAL";
            Kpi newKpi = null;
            try {
                newKpi = kpiFactory.getKpiByExecutable(0, 0, eng_arguments, description);
                kpiCatalog.addNewKpi(eng_arguments, description, newKpi.getKpiInfo().getExecutable());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            Kpi kpi = kpiFactory.getKpiById(0);
            List<String> arguments = Arrays.asList("");
            kpi.setArguments(arguments);*/


            gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
            String messageType="";
            List<KeyValue> entries = new LinkedList<>();
            for (Map.Entry<String, Object> entry : message.entrySet()) {
                String key = entry.getKey() != null ? entry.getKey() : "";
                String value = entry.getValue() != null ? entry.getValue().toString() : "";
                if (key.matches("payload")) {
                    value = value.replaceAll("=", "\":\"").replaceAll("\\s+", "").replaceAll(":\"\\[", ":[").replaceAll("\\{", "{\"").replaceAll(",", "\",\"").replaceAll("}", "\"}").replaceAll("}\",\"\\{", "},{").replaceAll("]\",", "],");
                    JsonObject payloadjson=new JsonParser().parse(value).getAsJsonObject();
                    Set<Map.Entry<String, JsonElement>> entrySet = payloadjson.entrySet();
                    for(Map.Entry<String,JsonElement> field : entrySet){
                        if (messageTypeNames.contains(field.getKey())) {
                            messageType=field.getKey();
                            entries.add(new KeyValue("topic",field.getKey()));
                            entries.add(new KeyValue("message","{\"" + field.getKey() + "\": " + field.getValue() + "}"));
                        }
                        else
                            // Is this needed?
                            entries.add(new KeyValue(field.getKey(),field.getValue().getAsString()));
                    }
                }
                else
                    // Is this needed?
                    entries.add(new KeyValue(key,value));
            }
            bdamessage.setEntries(entries);
            try {
                Entrypoint.datastore.insert(bdamessage);
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.log(Level.INFO,"Subscriber["+authHash+"], Received and persisted "+messageType+" message.");

            MessageType msgInfo = MessageType.getMessageByName(messageType);
            try {
                JobDescription job = JobDescription.getJobByMessageId(msgInfo.getId());
                LOGGER.log(Level.INFO,"Subscriber["+authHash+"], Launching "+job.getName()+" recipe.");
                /*List<String> messageArguments = Arrays.asList(bdamessage.toString());
                kpi.setArguments(messageArguments);
                (new Thread(kpi)).start();*/
            } catch (SQLException e) {
                LOGGER.log(Level.INFO,"Subscriber["+authHash+"], No recipe found for message "+messageType+".");
            }
        } else {
            // Original code for message type: `SonaeSalesForecast`.

            gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
            List<KeyValue> entries = new LinkedList<>();
            for (Map.Entry<String, Object> entry : message.entrySet()) {
                String key = entry.getKey() != null ? entry.getKey() : "";
                String value = entry.getValue() != null ? entry.getValue().toString() : "";
                if (key.matches("payload")) {
                    value = value.replaceAll("=", "\":\"").replaceAll("\\s+", "").replaceAll(":\"\\[", ":[").replaceAll("\\{", "{\"").replaceAll(",", "\",\"").replaceAll("}", "\"}").replaceAll("}\",\"\\{", "},{").replaceAll("]\",", "],");
                    JsonObject payloadjson=new JsonParser().parse(value).getAsJsonObject();
                    Set<Map.Entry<String, JsonElement>> entrySet = payloadjson.entrySet();
                    for(Map.Entry<String,JsonElement> field : entrySet){
                        if (field.getKey().matches("sales_forecast")) {
                            entries.add(new KeyValue("topic",field.getKey()));
                            entries.add(new KeyValue("message","{\"" + field.getKey() + "\": " + field.getValue() + "}"));
                        }
                        else
                            entries.add(new KeyValue(field.getKey(),field.getValue().getAsString()));
                    }
                }
                else
                    entries.add(new KeyValue(key,value));
            }
            bdamessage.setEntries(entries);
            try {
                Entrypoint.datastore.insert(bdamessage);
            } catch (Exception e) {
                e.printStackTrace();
            }

            LOGGER.log(Level.INFO,"Subscriber["+authHash+"], Received SalesForeCast message.");
        }
    }
}
