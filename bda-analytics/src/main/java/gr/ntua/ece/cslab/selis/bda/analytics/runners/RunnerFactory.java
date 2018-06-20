package gr.ntua.ece.cslab.selis.bda.analytics.runners;

import java.util.List;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsInstance;
import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;

public class RunnerFactory {
	public static RunnerFactory runnerFactory;

	private RunnerFactory() {}

	public static RunnerFactory getInstance() {
		if (runnerFactory == null)
			runnerFactory = new RunnerFactory();
		return runnerFactory;
	}
	public Runnable getRunner(KpiDescriptor kpi,
							  ExecutEngineDescriptor engine,
							  String message,
                              KPIBackend kpidb
	) {
		if (engine.isLocal_engine())
			return new LocalRunner(kpi, engine, message, kpidb);
		return null;
	};
}