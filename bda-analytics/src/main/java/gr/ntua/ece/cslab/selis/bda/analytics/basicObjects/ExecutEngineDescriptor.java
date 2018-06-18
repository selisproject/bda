package gr.ntua.ece.cslab.selis.bda.analytics.basicObjects;

import org.json.JSONObject;

public class ExecutEngineDescriptor {
	private String engineName;
	private String executionPreamble;
	private boolean local_engine;
	private JSONObject args;

	public ExecutEngineDescriptor() {
	}

	public ExecutEngineDescriptor(String engineName, String executionPreamble, boolean local_engine, JSONObject args) {
		this.engineName = engineName;
		this.executionPreamble = executionPreamble;
		this.local_engine = local_engine;
		this.args = args;
	}

	public String getEngineName() {
		return engineName;
	}

	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	public String getExecutionPreamble() {
		return executionPreamble;
	}

	public void setExecutionPreamble(String executionPreamble) {
		this.executionPreamble = executionPreamble;
	}

	public boolean isLocal_engine() {
		return local_engine;
	}

	public void setLocal_engine(boolean local_engine) {
		this.local_engine = local_engine;
	}

	public JSONObject getArgs() {
		return args;
	}

	public void setArgs(JSONObject args) {
		this.args = args;
	}
}
