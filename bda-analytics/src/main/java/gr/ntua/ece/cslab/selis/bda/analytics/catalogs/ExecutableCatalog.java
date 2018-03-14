package gr.ntua.ece.cslab.selis.bda.analytics.catalogs;

import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.Executable;

public class ExecutableCatalog {
	private static HashMap<Integer, Executable> executables;
	private static int executableCounter;
	private static ExecutableCatalog executableCatalog;

	private ExecutableCatalog() {
		executables = new HashMap<Integer, Executable>();
		executableCounter = 0;
	}

	public static ExecutableCatalog getInstance() {
		if (executableCatalog == null)
			executableCatalog = new ExecutableCatalog();
		return executableCatalog;
	}

	public Executable getExecutable(int executableID) {
		return executables.get(executableID);
	}

	public String getAllExecutables() {
		if (executables == null)
			return "";
		else
			return new Gson().toJson(executables);
	}

	public int getExecutableCounter() {
		return executableCounter;
	}

	public void addNewExecutable(List<String> argumentTypes, ExecutEngineDescriptor executEngine, String osPath, String description) {
		Executable newExecutable = new Executable(executEngine, argumentTypes, osPath,
				description);
		executables.put(executableCounter, newExecutable);
		executableCounter++;
	}
}
