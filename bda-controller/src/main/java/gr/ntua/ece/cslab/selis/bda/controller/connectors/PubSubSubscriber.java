package gr.ntua.ece.cslab.selis.bda.controller.connectors;

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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;

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
//                        sb.append(key).append("=").append(value).append(", ");
                        entries.add(new KeyValue(key,value));
                    }
                    bdamessage.setEntries(entries);
                    try {
                        Entrypoint.myBackend.insert(bdamessage);
                        AnalyticsSystem mySystem = AnalyticsSystem.getInstance();
                        ExecutEngineCatalog executEngineCatalog = ExecutEngineCatalog.getInstance();
                        ExecutableCatalog executableCatalog = mySystem.getExecutableCatalog();
                        KpiCatalog kpiCatalog = mySystem.getKpiCatalog();
                        KpiFactory kpiFactory = mySystem.getKpiFactory();
                        executEngineCatalog.addNewExecutEngine("spark", "spark-submit");
                        List<String> argtypes = Arrays.asList();
                        executableCatalog.addNewExecutable(argtypes, executEngineCatalog.getExecutEngine(0), "/home/hduser/jdbc.py ",
                                "This calculates 0");
                        List<String> arguments = Arrays.asList("--driver-class-path", "/home/hduser/postgresql-42.2.1.jar", "--jars", "/home/hduser/postgresql-42.2.1.jar");
                        String description = "This calculates shit done by blue trucks...";
                        Kpi newKpi = kpiFactory.getKpiByExecutable(0,0, arguments, description);
                        kpiCatalog.addNewKpi(arguments, description, newKpi.getKpiInfo().getExecutable());
                        Kpi kpi = kpiFactory.getKpiById(0);
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
