package gr.ntua.ece.cslab.selis.bda.analytics.basicObjects;
/**
 * @author nchalv
 * This class implements the descriptor of a single KPI Primitive. KPI Primitives describe the binaries which are executed
 * when a KPI is calculated. The class involves information about the execution engine, the argument types it expects, a textual
 * description and the path to the binary.
 */



import java.util.List;

import com.google.gson.Gson;
import org.json.JSONObject;

public class Executable {
	private int engineID;
	private JSONObject args;
	private String osPath;

	public Executable() {
	}

	public Executable(int engineID, JSONObject args, String osPath) {
		this.engineID = engineID;
		this.args = args;
		this.osPath = osPath;
	}

	public int getEngineID() {
		return engineID;
	}

	public void setEngineID(int engineID) {
		this.engineID = engineID;
	}

	public JSONObject getArgs() {
		return args;
	}

	public void setArgs(JSONObject args) {
		this.args = args;
	}

	public String getOsPath() {
		return osPath;
	}

	public void setOsPath(String osPath) {
		this.osPath = osPath;
	}
}
