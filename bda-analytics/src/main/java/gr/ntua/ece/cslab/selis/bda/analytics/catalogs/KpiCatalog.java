package gr.ntua.ece.cslab.selis.bda.analytics.catalogs;

import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiPrimitiveDescriptor;

public class KpiCatalog {
	private static HashMap<Integer, KpiDescriptor> kpis;
	private static int kpiCounter;
	private static KpiCatalog kpiCatalog;

	private KpiCatalog() {
		kpis = new HashMap<Integer, KpiDescriptor>();
		kpiCounter = 0;
	}

	public static KpiCatalog getInstance() {
		if (kpiCatalog == null)
			kpiCatalog = new KpiCatalog();
		return kpiCatalog;
	}

	public KpiDescriptor getKpi(int kpiID) {
		return kpis.get(kpiID);
	}

	public String getAllKpis() {
		if (kpis == null)
			return "";
		else
			return new Gson().toJson(kpis);
	}

	public int getKpiCounter() {
		return kpiCounter;
	}

	public void addNewKpi(List<String> arguments, String description, KpiPrimitiveDescriptor kpiPrimitiveDescriptor) {
		KpiDescriptor newKpi = new KpiDescriptor(description, kpiPrimitiveDescriptor, arguments);
		kpis.put(kpiCounter, newKpi);
		kpiCounter++;
	}
}