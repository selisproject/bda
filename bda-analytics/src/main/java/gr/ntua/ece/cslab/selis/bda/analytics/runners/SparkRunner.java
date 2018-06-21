package gr.ntua.ece.cslab.selis.bda.analytics.runners;

import org.apache.spark.launcher.SparkLauncher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KeyValue;
import org.json.JSONObject;

public class SparkRunner extends ArgumentParser implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(SparkLauncher.class.getCanonicalName());

    String engine_part;
    String recipe_part;
    KpiDescriptor kpiDescriptor;
    ExecutEngineDescriptor engine;
    KPIBackend kpidb;
    String message;

    public SparkRunner(KpiDescriptor kpi,
                       ExecutEngineDescriptor engine,
                       String message,
                       KPIBackend kpidb) {
        this.kpiDescriptor = kpi;
        this.engine = engine;
        this.message = message;
        this.kpidb = kpidb;

        engine_part = "";
        recipe_part = "";

        // Set first the path of the engine
        engine_part += engine.getExecutionPreamble();

        if (engine.getArgs().length() != 0) {
            // Add code to support engine arguments
        }

        // Set the path of the recipe executable
        recipe_part += kpi.getExecutable().getOsPath();
    }

    @Override
    public void run() {
        Process spark = null;
        try {
            spark = new SparkLauncher()
                    .setMaster(engine_part)
                    .setDeployMode("cluster")
                    .setAppResource(recipe_part)
                    //.redirectOutput(new File("/results/" + kpiDescriptor.getName() + ".out"))
                    .addAppArgs(message).launch();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job failed to start!");
        }
        try {
            spark.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job execution was interrupted!");
        }
        /*try {
            store("/results/" + kpiDescriptor.getName() + ".out");
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        spark.destroy();
        LOGGER.log(Level.INFO,"Spark job finished!");
    }

    private void store(String outputpath) throws Exception {
        JSONObject msg = new JSONObject(this.message);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(outputpath));
        String line = null;
        StringBuffer sb = new StringBuffer();
        try {
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        String result = sb.toString();
        result = result.replaceAll("\\s", "");
        LOGGER.log(Level.INFO,result);
        List<KeyValue> entries = new ArrayList<>();
        for (Iterator<String> it = msg.getJSONObject("payload").keys(); it.hasNext(); ) {
            String key = it.next();
            entries.add(new KeyValue(key, msg.getJSONObject("payload").get(key).toString()));
        }
        entries.add(new KeyValue("result", (new JSONObject(result)).toString()));
        kpidb.insert(new KPI(kpiDescriptor.getName(), (new Timestamp(System.currentTimeMillis())).toString(), entries));

    }
}
