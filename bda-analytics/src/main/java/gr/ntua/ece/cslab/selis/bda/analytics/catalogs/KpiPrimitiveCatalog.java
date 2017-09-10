package gr.ntua.ece.cslab.selis.bda.analytics.catalogs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiPrimitiveDescriptor;

public class KpiPrimitiveCatalog {
	private static HashMap<Integer, KpiPrimitiveDescriptor> kpiPrimitives;
	private static int kpiPrimitiveCounter;
	private static KpiPrimitiveCatalog kpiPrimitiveCatalog;

	private KpiPrimitiveCatalog() {
		kpiPrimitives = new HashMap<Integer, KpiPrimitiveDescriptor>();
		kpiPrimitiveCounter = 0;
	}

	public static KpiPrimitiveCatalog getInstance() {
		if (kpiPrimitiveCatalog == null)
			kpiPrimitiveCatalog = new KpiPrimitiveCatalog();
		return kpiPrimitiveCatalog;
	}

	public KpiPrimitiveDescriptor getKpiPrimitive(int kpiPrimitiveID) {
		return kpiPrimitives.get(kpiPrimitiveID);
	}

	public String getAllKpiPrimitives() {
		if (kpiPrimitives == null)
			return "";
		else
			return new Gson().toJson(kpiPrimitives);
	}

	public int getKpiPrimitiveCounter() {
		return kpiPrimitiveCounter;
	}

	public void addNewKpiPrimitive(List<String> argumentTypes, int executEngine, String osPath, String description) {
		KpiPrimitiveDescriptor newKpiPrimitive = new KpiPrimitiveDescriptor(executEngine, argumentTypes, osPath,
				description);
		kpiPrimitives.put(kpiPrimitiveCounter, newKpiPrimitive);
		kpiPrimitiveCounter++;
	}
}
