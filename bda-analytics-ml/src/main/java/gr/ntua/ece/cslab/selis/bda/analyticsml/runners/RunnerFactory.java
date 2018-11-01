package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;

public class RunnerFactory {
	public static RunnerFactory runnerFactory;

	private RunnerFactory() {}

	public static RunnerFactory getInstance() {
		if (runnerFactory == null)
			runnerFactory = new RunnerFactory();
		return runnerFactory;
	}
	public Runnable getRunner(Recipe recipe,
							  ExecutionEngine engine,
							  String message,
                              String scnSlug
	) {

		if (engine.isLocal_engine())
			return new LocalRunner(recipe, engine, message, scnSlug);
		else
			return new SparkRunner(recipe, engine, message, scnSlug);
	}
}