package gr.ntua.ece.cslab.selis.bda.analytics.basicObjects;

public class ExecutEngineDescriptor {
	private int engineID;
	private String engineName;

	public ExecutEngineDescriptor(int systemID, String systemName) {
		this.engineID = systemID;
		this.engineName = systemName;
	}

	public int getSystemID() {
		return engineID;
	}

	public String getSystemName() {
		return engineName;
	}

	@Override
	public String toString() {
		return "ExecutEngineDescriptor [engineID=" + engineID + ", engineName" + engineName + "]";
	}

}
