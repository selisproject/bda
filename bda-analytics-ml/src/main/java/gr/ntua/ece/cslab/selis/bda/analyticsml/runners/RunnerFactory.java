/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Job;
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
                              Job job,
                              String scnSlug
	) throws Exception {

		if (engine.isLocal_engine())
			return new LocalRunner(recipe, engine, messageId, scnSlug);
		else if (engine.getName().matches("spark-livy"))
			return new LivyRunner(recipe, msgInfo, messageId, job, scnSlug);
		else
			throw new Exception("Unknown engine type. Could not relate to existing runners.");
	}
}