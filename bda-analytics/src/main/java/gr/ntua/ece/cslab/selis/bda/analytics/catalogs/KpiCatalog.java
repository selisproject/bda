package gr.ntua.ece.cslab.selis.bda.analytics.catalogs;

import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.Executable;

public class KpiCatalog {
	private HashMap<Integer, KpiDescriptor> kpis;

	public KpiCatalog() {
		kpis = new HashMap<Integer, KpiDescriptor>();
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

	public void addNewKpi(int recipe_id, String name, String description, int engine_id,
						  String args, String ospath) {
		KpiDescriptor newKpi = new KpiDescriptor(name, description,
				new Executable(engine_id, args, ospath));
		kpis.put(recipe_id, newKpi);
	}
}
