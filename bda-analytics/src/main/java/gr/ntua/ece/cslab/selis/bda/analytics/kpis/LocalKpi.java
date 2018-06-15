package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KeyValue;

public class LocalKpi extends ArgumentParser implements Runnable {

    String engine_part;
    String recipe_part;
    KpiDescriptor kpiDescriptor;
    ExecutEngineDescriptor engine;
    String message;

    public LocalKpi(KpiDescriptor kpi,
                    ExecutEngineDescriptor engine,
                    String message) {
        this.kpiDescriptor = kpi;
        this.engine = engine;
        this.message = message;

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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void store(HashMap<String, String> hmap) throws Exception {
		String fs_string = "jdbc:postgresql://10.0.1.4:5432/sonae";
		String uname = "clms";
		String passwd = "sonae@sEl1s";

		List<KeyValue> data = new LinkedList<KeyValue>();
		hmap.forEach((k, v) -> data.add(new KeyValue(k, v)));
//		for	(KeyValue element : data) {
//			System.out.println(element.getKey());
//			System.out.println(element.getValue());
//		}
		KPI newkpi = new KPI("sonae_orderforecast", (new Timestamp(System.currentTimeMillis())).toString(), data);

		//kpiDB.insert(newkpi);

		//kpiDB.stop();
	}

}
