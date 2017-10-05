package gr.ntua.ece.cslab.selis.bda.ml.basicObjects;

public class SystemDescriptor {
	private int systemID;
	private String systemName;
	
	public SystemDescriptor(int systemID, String systemName) {
		super();
		this.systemID = systemID;
		this.systemName = systemName;
	}

	public int getSystemID() {
		return systemID;
	}
	
	public String getSystemName() {
		return systemName;
	}

	@Override
	public String toString() {
		return "SystemDescriptor [systemID=" + systemID + ", systemName=" + systemName + "]";
	}
	
	
	
}
