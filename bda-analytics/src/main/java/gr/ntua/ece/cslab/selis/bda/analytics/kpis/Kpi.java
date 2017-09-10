package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.util.ArrayList;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;

public class Kpi {


	private int kpiID;
	private KpiDescriptor kpiInfo;
	
	public Kpi() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Kpi(int kpiID, KpiDescriptor kpiInfo) {
		super();
		this.kpiID = kpiID;
		this.kpiInfo = kpiInfo;
	}

	
	public int getKpiID() {
		return kpiID;
	}

	public KpiDescriptor getKpiInfo() {
		return kpiInfo;
	}
	
	public int calculate(int executEngine, List<String> argNames, ArrayList<ArrayList<Double>> dataset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void store(int value) {
		// TODO Auto-generated method stub

	}

}
