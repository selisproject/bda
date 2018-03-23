package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;

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
			String result = "";
			// System.out.println("Inside");
			Runtime r = Runtime.getRuntime();
			String command = kpiInfo.getExecutable().getExecutEngine().getExecutionPreamble() + " "
					+ kpiInfo.getExecutable().getOsPath();
			Process p;
			// System.out.println("Inside");

			// p = r.exec(command);
			p = r.exec("echo \"hi\"");
			p.waitFor();
			// System.out.println("Dead");

			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
				result += inputLine;
			}
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void store(int value) {
		// TODO Auto-generated method stub

	}

}
