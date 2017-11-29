package gr.ntua.ece.cslab.selis.bda.analytics.catalogs;

import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;

public class ExecutEngineCatalog {
	private static HashMap<Integer, ExecutEngineDescriptor> executEngines;
	private static int executEnginesCounter;
	private static ExecutEngineCatalog executEngineCatalog;

	private ExecutEngineCatalog() {
		executEngines = new HashMap<Integer, ExecutEngineDescriptor>();
		executEnginesCounter = 0;
	}

	public static ExecutEngineCatalog getInstance() {
		if (executEngineCatalog == null)
			executEngineCatalog = new ExecutEngineCatalog();
		return executEngineCatalog;
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

	public int getExecutEnginesCounter() {
		return executEnginesCounter;
	}

	public void addNewExecutEngine(String engineName, String executionPreamble) {
		ExecutEngineDescriptor newExecutEngineDescriptor = new ExecutEngineDescriptor(engineName, executionPreamble);
		executEngines.put(executEnginesCounter, newExecutEngineDescriptor);
		executEnginesCounter++;}
}
