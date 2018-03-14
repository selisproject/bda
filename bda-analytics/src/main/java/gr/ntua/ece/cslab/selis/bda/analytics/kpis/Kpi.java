package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;

public class Kpi {


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
	
	public int calculate() {
		// TODO Auto-generated method stub
		String result = "";
		try {
		    Runtime r = Runtime.getRuntime();                    
		    String command = kpiInfo.getExecutable().getExecutEngine().getExecutionPreamble()+" "+ kpiInfo.getExecutable().getOsPath();
		    
		    Process p = r.exec(command);
		    p.waitFor();
		    BufferedReader in =
		        new BufferedReader(new InputStreamReader(p.getInputStream()));
		    String inputLine;
		    while ((inputLine = in.readLine()) != null) {
		        System.out.println(inputLine);
		        result += inputLine;
		    }
		    in.close();

		} catch (Exception e) {
		    System.out.println(e);
		}
		return Integer.parseInt(result);
	}

	public void store(int value) {
		// TODO Auto-generated method stub

	}

}
