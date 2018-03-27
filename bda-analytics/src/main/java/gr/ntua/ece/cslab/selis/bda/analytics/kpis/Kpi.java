package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.datastore.KPIBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KPIDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import java.util.LinkedList;

public class Kpi implements Runnable {

	private KpiDescriptor kpiInfo;

	public Kpi() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Kpi(int kpiID, KpiDescriptor kpiInfo) {
		super();
		this.kpiInfo = kpiInfo;
	}

	public KpiDescriptor getKpiInfo() {
		return kpiInfo;
	}

	public void run() {
		try {
			/*
			 * String result = ""; // System.out.println("Inside"); Runtime r =
			 * Runtime.getRuntime(); String command =
			 * kpiInfo.getExecutable().getExecutEngine().getExecutionPreamble() + " " +
			 * kpiInfo.getExecutable().getOsPath(); Process p; //
			 * System.out.println("Inside");
			 * 
			 * p = r.exec(command); //p = r.exec("echo \"hi\""); p.waitFor(); //
			 * System.out.println("Dead");
			 * 
			 * BufferedReader in = new BufferedReader(new
			 * InputStreamReader(p.getInputStream())); String inputLine; while ((inputLine =
			 * in.readLine()) != null) { System.out.println(inputLine); result += inputLine;
			 * } in.close();
			 */
			List<String> cmd = new ArrayList<String>(getKpiInfo().getArguments());
			cmd.add(0, kpiInfo.getExecutable().getExecutEngine().getExecutionPreamble());
			cmd.add(kpiInfo.getExecutable().getOsPath());
			ProcessBuilder pb = new ProcessBuilder(cmd);
			// pb.directory(new File("/home/hduser/nchalv/"));
			// System.out.println(pb.directory());
			// File err = new File("err");
			// File out = new File("out");
			// pb.redirectError(Redirect.appendTo(err));
			// pb.redirectOutput(Redirect.appendTo(out));
			Process p = pb.start();
			// assert pb.redirectInput() == Redirect.PIPE;
			// assert pb.redirectOutput().file() == log;
			// assert p.getInputStream().read() == -1;
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			StringBuilder builder = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("FINAL RESULT: ")) {
					builder.append(line);
					builder.append(System.getProperty("line.separator"));
				}
			}
			String result = builder.toString();
			p.waitFor();
			// System.out.println(result);
			result = result.replaceAll("FINAL RESULT: ", "");
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
			this.store(hmap);
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

		KPIBackend kpiDB = new KPIBackend(fs_string, uname, passwd);
		List<KeyValue> data = new LinkedList<KeyValue>();
		hmap.forEach((k, v) -> data.add(new KeyValue(k, v)));
//		for	(KeyValue element : data) {
//			System.out.println(element.getKey());
//			System.out.println(element.getValue());
//		}
		KPIDescription newkpi = new KPIDescription("sonae_orderforecast", System.currentTimeMillis(), data);

		kpiDB.insert(newkpi);

		kpiDB.stop();
	}

}
