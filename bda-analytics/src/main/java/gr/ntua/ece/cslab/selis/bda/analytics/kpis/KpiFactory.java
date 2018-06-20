package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import java.util.List;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsInstance;
import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;

public class KpiFactory {
	public static KpiFactory kpiFactory;

	private KpiFactory() {}

	public static KpiFactory getInstance() {
		if (kpiFactory == null)
			kpiFactory = new KpiFactory();
		return kpiFactory;
	}
	public Runnable getRunner(KpiDescriptor kpi,
							  ExecutEngineDescriptor engine,
							  String message,
                              KPIBackend kpidb
	) {
		if (engine.isLocal_engine())
			return new LocalKpi(kpi, engine, message, kpidb);
		return null;
	};
}