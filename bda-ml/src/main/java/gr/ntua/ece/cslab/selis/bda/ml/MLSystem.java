package gr.ntua.ece.cslab.selis.bda.ml;

import java.util.List;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.AlgorithmDescriptor;
import gr.ntua.ece.cslab.selis.bda.ml.catalogs.AlgorithmCatalog;
import gr.ntua.ece.cslab.selis.bda.ml.catalogs.ModelCatalog;

public class MLSystem implements MLInterface {

	private static MLSystem system;
	
	private MLSystem() {	}
	
	public static MLSystem getInstance() {
		if (system == null)
			system = new MLSystem();
		return system;
		
	}

	public AlgorithmCatalog getAlgoCatalog() {
		return AlgorithmCatalog.getInstance();
	}

	public ModelCatalog getModelCatalog() {
		return ModelCatalog.getInstance();
	}

	public void getModelFactory() {
		// TODO Auto-generated method stub

	}
	
	public static void main(String[] args) {
		MLSystem mySystem = MLSystem.getInstance();
		AlgorithmCatalog algoCat = mySystem.getAlgoCatalog();
		List<AlgorithmDescriptor> algorithms = algoCat.getAllAlgorithms();
		
		for (AlgorithmDescriptor a : algorithms) {
			System.out.println(a.toString());
		}

	}

}
