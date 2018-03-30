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
import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import de.tu_dresden.selis.pubsub.*;
import de.tu_dresden.selis.pubsub.PubSubException;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOG = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");
    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private static List<String> rules;

    public PubSubSubscriber(String authHash, String hostname, int portNumber, List<String> rules) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;
        this.rules = rules;
    }

    @Override
    public void run() {

        try (PubSub c = new PubSub(this.hostname, this.portNumber)) {
            AnalyticsSystem mySystem = AnalyticsSystem.getInstance();
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
            final Kpi kpi = kpiFactory.getKpiById(0);

            Subscription subscription1 = new Subscription(this.authHash);
            Subscription subscription2 = new Subscription(this.authHash);
            //this line can throw exception if we provide value of invalid type. Check ValueType for allowed values
            //for (String rule : this.rules){
            subscription1.add(new Rule("message_type", "StockLevelUpdate", RuleType.EQ));
            subscription2.add(new Rule("message_type", "SalesForeCast", RuleType.EQ));
            //}

            c.subscribe(subscription1, new Callback() {
                @Override
                public void onMessage(Message message) {
                    gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
                    List<KeyValue> entries = new LinkedList<>();
//                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<String, Object> entry : message.entrySet()) {
                        String key = entry.getKey() != null ? entry.getKey() : "";
                        String value = entry.getValue() != null ? entry.getValue().toString() : "";
                        if (key.matches("payload")) {
                            value = value.replaceAll("=", "\":\"").replaceAll("\\s+", "").replaceAll(":\"\\[", ":[").replaceAll("\\{", "{\"").replaceAll(",", "\",\"").replaceAll("}", "\"}").replaceAll("}\",\"\\{", "},{").replaceAll("]\",", "],");
                            JsonObject payloadjson=new JsonParser().parse(value).getAsJsonObject();
                            Set<Map.Entry<String, JsonElement>> entrySet = payloadjson.entrySet();
                            for(Map.Entry<String,JsonElement> field : entrySet){
                                if ((field.getKey().matches("sales_forecast")) || (field.getKey().matches("stock_levels"))) {
                                    entries.add(new KeyValue("topic",field.getKey()));
                                    entries.add(new KeyValue("message","{\"" + field.getKey() + "\": " + field.getValue() + "}"));
                                }
                                else
                                    entries.add(new KeyValue(field.getKey(),field.getValue().getAsString()));
                            }
                        }
                        else
//                            sb.append(key).append("=").append(value).append(", ");
                            entries.add(new KeyValue(key,value));
                    }
                    bdamessage.setEntries(entries);
                    try {
                        System.out.println(bdamessage.toString());
                        Entrypoint.myBackend.insert(bdamessage);
                        //List<String> arguments = Arrays.asList("insert argument here");
                        //kpi.setArguments(arguments);
                        //(new Thread(kpi)).start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    LOG.log(Level.INFO,"Subscriber["+authHash+"], Received StockLevelUpdate message.");
                }
            });

            c.subscribe(subscription2, new Callback() {
                @Override
                public void onMessage(Message message) {
                    gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
                    List<KeyValue> entries = new LinkedList<>();
//                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<String, Object> entry : message.entrySet()) {
                        String key = entry.getKey() != null ? entry.getKey() : "";
                        String value = entry.getValue() != null ? entry.getValue().toString() : "";
//                        sb.append(key).append("=").append(value).append(", ");
                        entries.add(new KeyValue(key,value));
                    }
                    bdamessage.setEntries(entries);
                    try {
                        Entrypoint.myBackend.insert(bdamessage);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    LOG.log(Level.INFO,"Subscriber["+authHash+"], Received SalesForeCast message.");
                }
            });

            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (PubSubException ex) {
            LOG.log(Level.WARNING,"Could not subscribe, got error: {}", ex.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.log(Level.INFO,"Finishing");
    }
}
