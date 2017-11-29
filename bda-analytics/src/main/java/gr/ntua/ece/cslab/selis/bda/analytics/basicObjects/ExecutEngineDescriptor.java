package gr.ntua.ece.cslab.selis.bda.analytics.basicObjects;

public class ExecutEngineDescriptor {
	private int engineID;
	private String engineName;
	private String executionPreamble;


	public ExecutEngineDescriptor(String engineName, String executionPreamble) {
		this.engineName = engineName;
		this.executionPreamble = executionPreamble;
	}


	public String getEngineName() {
		return engineName;
	}
	
	public String getExecutionPreamble() {
		return executionPreamble;
	}

	@Override
	public String toString() {
		return "ExecutEngineDescriptor [engineName" + engineName + ", executionPreamble"+executionPreamble+"]";
	}

}
