package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
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
							  MessageType msgInfo,
							  String messageId,
                              JobDescription job,
                              String scnSlug
	) throws Exception {

		if (engine.isLocal_engine())
			return new LocalRunner(recipe, engine, messageId, scnSlug);
		else if (engine.getName().matches("spark"))
			return new SparkRunner(recipe, engine, messageId, scnSlug);
		else if (engine.getName().matches("livy"))
			return new LivyRunner(recipe, msgInfo, messageId, job, scnSlug);
		else
			throw new Exception("Unknown engine type. Could not relate to existing runners.");
	}
}