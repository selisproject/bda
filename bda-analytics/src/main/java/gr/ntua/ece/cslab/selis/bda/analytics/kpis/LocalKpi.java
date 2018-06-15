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

        // Set primary input - the message
        //recipe_part += message + " ";

        //recipe_part += get_executable_arguments(kpi.getExecutable().getArgs());
    }

	public void run() {
		try {

				/*List<String> cmd = new ArrayList<String>(getKpiInfo().getEng_arguments());
				cmd.add(0, kpiInfo.getExecutable().getExecutEngine().getExecutionPreamble());
				cmd.add(kpiInfo.getExecutable().getOsPath());
				List<String> fcmd = Stream.concat(cmd.stream(), this.arguments.stream()).collect(Collectors.toList());
				*/
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
			// pb.directory(new File("/home/hduser/nchalv/"));
			// System.out.println(pb.directory());
			// File err = new File("err");
			// File out = new File("out");
			// pb.redirectError(Redirect.appendTo(err));
			// pb.redirectOutput(Redirect.appendTo(out));
			/*
				Process p = pb.start();
			*/
			//BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			//StringBuilder builder = new StringBuilder();
			//String line = null;
			//  while ((line = reader.readLine()) != null) {
			//	if (line.startsWith("FINAL RESULT: ")) {
			//		builder.append(line);
			//		builder.append(System.getProperty("line.separator"));
			//	}
			//}
			//String result = builder.toString();
			//	p.waitFor();
			// System.out.println(result);
			/*result = result.replaceAll("FINAL RESULT: ", "");
			// System.out.println(result);
			JSONParser parser = new JSONParser();
			JSONObject jsonObj = (JSONObject) parser.parse(result);
			HashMap<String, String> hmap = new HashMap<String, String>();
			for (Object key : jsonObj.keySet()) {
				// based on you key types
				String keyStr = (String) key;
				Object keyvalue = jsonObj.get(keyStr);
				hmap.put(keyStr, keyvalue.toString());
				// Print key and value
				// System.out.println("key: "+ keyStr + " value: " + keyvalue);

				// for nested objects iteration if required
				// if (keyvalue instanceof JSONObject)
				// printJsonObject((JSONObject)keyvalue);
			}
			this.store(hmap);*/
			//hmap.forEach((k, v) -> System.out.println("key: " + k + " value:" + v));
			// System.out.println(hmap);
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
