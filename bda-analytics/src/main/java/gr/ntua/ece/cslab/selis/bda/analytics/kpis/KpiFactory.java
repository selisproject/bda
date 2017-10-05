package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.util.List;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiPrimitiveDescriptor;

public class KpiFactory {

	private static KpiFactory kpiFactory;

	private KpiFactory() {
		super();
	}

	public static KpiFactory getInstance() {
		if (kpiFactory == null)
			kpiFactory = new KpiFactory();
		return kpiFactory;
	}

	public Kpi getKpiById(int kpiID) throws Exception {
		KpiDescriptor kpi = AnalyticsSystem.getInstance().getKpiCatalog().getKpi(kpiID);
		return new Kpi(kpiID, kpi);
	}
	public Kpi getKpiByPrim(int kpiID, int kpiPrimitiveID, List<String> arguments, String description)throws Exception {
		KpiPrimitiveDescriptor kpiPrimitiveDescriptor = AnalyticsSystem.getInstance().getKpiPrimitiveCatalog().getKpiPrimitive(kpiPrimitiveID);

		return new Kpi(kpiID, new KpiDescriptor(description, kpiPrimitiveDescriptor, arguments));
	}

}