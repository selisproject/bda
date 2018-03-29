package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.util.List;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.Executable;

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
	public Kpi getKpiByExecutable(int kpiID, int kpiPrimitiveID,List<String> eng_arguments, List<String> arguments, String description)throws Exception {
		Executable kpiPrimitiveDescriptor = AnalyticsSystem.getInstance().getExecutableCatalog().getExecutable(kpiPrimitiveID);

		return new Kpi(kpiID, new KpiDescriptor(description, kpiPrimitiveDescriptor, eng_arguments, arguments));
	}

}