package gr.ntua.ece.cslab.selis.bda.ml.catalogs;

import java.util.ArrayList;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.AlgorithmDescriptor;
import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.SystemDescriptor;

public class AlgorithmCatalog {
	
	private List<AlgorithmDescriptor> algorithms;
	
	private static AlgorithmCatalog algorithmCatalog;

	private AlgorithmCatalog() {
		algorithms = new ArrayList<AlgorithmDescriptor>();
		SystemDescriptor system1 = new SystemDescriptor(1, "R");
		SystemDescriptor system2 = new SystemDescriptor(2, "Spark");
		SystemDescriptor system3 = new SystemDescriptor(3, "Tensorflow");
		
		List<SystemDescriptor> systemList1 = new ArrayList<SystemDescriptor>();
		systemList1.add(system1);
		systemList1.add(system2);
		systemList1.add(system3);
		
		algorithms.add(new AlgorithmDescriptor("linear regression", systemList1));
		
		List<SystemDescriptor> systemList2 = new ArrayList<SystemDescriptor>();
		systemList2.add(system3);
		
		algorithms.add(new AlgorithmDescriptor("regression ANN", systemList2));
		
		
	}
	
	public static AlgorithmCatalog getInstance() {
		if (algorithmCatalog == null)
				algorithmCatalog = new AlgorithmCatalog();
		return algorithmCatalog;
	}
	
	public List<AlgorithmDescriptor> getAllAlgorithms() {
		return algorithms;
	}
	
}
