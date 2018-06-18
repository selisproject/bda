package gr.ntua.ece.cslab.selis.bda.analytics.catalogs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import org.json.JSONObject;

public class ExecutEngineCatalog {
	private HashMap<Integer, ExecutEngineDescriptor> executEngines;

	public ExecutEngineCatalog() {
		executEngines = new HashMap<Integer, ExecutEngineDescriptor>();
	}

	public ExecutEngineDescriptor getExecutEngine(int executEngineID) {
		return executEngines.get(executEngineID);
	}

	public String getAllExecutEngines() {
		if (executEngines == null)
			return "";
		else
			return new Gson().toJson(executEngines);
	}

	public void initialize(ResultSet engines) {
		if (engines != null) {
			try {
				while (engines.next()) {
					addNewExecutEngine(
							engines.getInt("id"),
							engines.getString("name"),
							engines.getString("engine_path"),
							engines.getBoolean("local_engine"),
							new JSONObject(engines.getString("args"))
					);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public int getExecutEnginesCounter() {
		return executEngines.size();
	}

	public void addNewExecutEngine(int engineId, String engineName, String executionPreamble,
								   boolean local_engine, JSONObject args) {
		ExecutEngineDescriptor newExecutEngineDescriptor = new ExecutEngineDescriptor(
			engineName, executionPreamble, local_engine, args
		);
		executEngines.put(engineId, newExecutEngineDescriptor);
	}
}
