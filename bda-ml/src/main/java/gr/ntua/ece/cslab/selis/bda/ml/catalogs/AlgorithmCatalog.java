package gr.ntua.ece.cslab.selis.bda.ml.catalogs;

import java.util.HashMap;

public class AlgorithmCatalog {
	
	private HashMap<Integer, String> algorithms;
	
	private AlgorithmCatalog algorithmCatalog;

	private AlgorithmCatalog() {
		algorithms = new HashMap<Integer, String>();
		algorithms.put(1, "algo1");
		algorithms.put(2, "algo2");
	}
	
	public AlgorithmCatalog getInstance() {
		if (algorithmCatalog == null)
				algorithmCatalog = new AlgorithmCatalog();
		return algorithmCatalog;
	}
	
	public String getAllAlgorithms() {
		if (algorithms == null)
			return "";
		else
			return algorithms.toString();
	}
	
}
