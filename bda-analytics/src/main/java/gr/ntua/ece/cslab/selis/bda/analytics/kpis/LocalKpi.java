package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsInstance;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KeyValue;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;

public class LocalKpi extends ArgumentParser implements Runnable {

    String engine_part;
    String recipe_part;
    KpiDescriptor kpiDescriptor;
    ExecutEngineDescriptor engine;
    KPIBackend kpidb;
    String message;

    public LocalKpi(KpiDescriptor kpi,
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

    public void run() {
        try {

            System.out.println(engine_part);
            System.out.println(recipe_part);
            ProcessBuilder pb = new ProcessBuilder(Arrays.asList(
                    engine_part, recipe_part, message,
                    get_executable_arguments(kpiDescriptor.getExecutable().getArgs())));
            File out = new File("/results/" + kpiDescriptor.getName() + ".out");
            pb.redirectError(ProcessBuilder.Redirect.to(new File(
                    "/results/" + kpiDescriptor.getName() + ".err")));
            pb.redirectOutput(ProcessBuilder.Redirect.to(out));
            Process p = pb.start();
            p.waitFor();
            store("/results/" + kpiDescriptor.getName() + ".out");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void store(String outputpath) throws Exception {

//		KPI newkpi = new KPI("sonae_orderforecast", (new Timestamp(System.currentTimeMillis())).toString(), data);
        JSONObject msg = new JSONObject(this.message);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(outputpath));
        String line = null;
        StringBuffer sb = new StringBuffer();
        try {
            while ((line = bufferedReader.readLine()) != null) {
                //System.out.println(line);
                sb.append(line);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        String result = sb.toString();
        result = result.replaceAll("\\s", "");
        System.out.println(result);
        List<KeyValue> entries = new ArrayList<>();
        for (Iterator<String> it = msg.getJSONObject("payload").keys(); it.hasNext(); ) {
            String key = it.next();
            entries.add(new KeyValue(key, msg.getJSONObject("payload").get(key).toString()));
        }
        entries.add(new KeyValue("result", (new JSONObject(result)).toString()));
        kpidb.insert(new KPI(kpiDescriptor.getName(), (new Timestamp(System.currentTimeMillis())).toString(), entries));

    }
}
