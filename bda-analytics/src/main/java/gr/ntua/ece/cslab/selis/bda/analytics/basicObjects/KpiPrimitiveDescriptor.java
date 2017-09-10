package gr.ntua.ece.cslab.selis.bda.analytics.basicObjects;
/**
 * @author nchalv
 * This class implements the descriptor of a single KPI Primitive. KPI Primitives describe the binaries which are executed
 * when a KPI is calculated. The class involves information about the execution engine, the argument types it expects, a textual
 * description and the path to the binary.
 */



import java.util.List;

import com.google.gson.Gson;

public class KpiPrimitiveDescriptor {
	private int executEngine;
	private List<String> argumentTypes;
	private String description;
	private String osPath;
	
	public KpiPrimitiveDescriptor(int executEngine, List<String> argumentTypes, String osPath, String description) {
		this.executEngine = executEngine;
		this.argumentTypes = argumentTypes;
		this.osPath = osPath;
		this.description = description;
	}

	public int getExecutEngine() {
		return executEngine;
	}

	public List<String> getArgumentTypes() {
		return argumentTypes;
	}

	public String getOsPath() {
		return osPath;
	}
	
	public String getDescription() {
		return description;
	}

	public String toJson() {
		return new Gson().toJson(this);
	}

}
