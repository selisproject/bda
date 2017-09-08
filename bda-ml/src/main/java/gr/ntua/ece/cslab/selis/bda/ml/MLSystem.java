package gr.ntua.ece.cslab.selis.bda.ml;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.AlgorithmDescriptor;
import gr.ntua.ece.cslab.selis.bda.ml.catalogs.AlgorithmCatalog;
import gr.ntua.ece.cslab.selis.bda.ml.catalogs.ModelCatalog;
import gr.ntua.ece.cslab.selis.bda.ml.models.Model;
import gr.ntua.ece.cslab.selis.bda.ml.models.ModelFactory;

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

	public ModelFactory getModelFactory() {
		return ModelFactory.getInstance();

	}
	
	public static void main(String[] args) {
		MLSystem mySystem = MLSystem.getInstance();
		AlgorithmCatalog algoCat = mySystem.getAlgoCatalog();
		
		List<AlgorithmDescriptor> algorithms = algoCat.getAllAlgorithms();
		
		for (AlgorithmDescriptor a : algorithms) {
			System.out.println(a.toString());
		}

		ModelFactory modelFactory = mySystem.getModelFactory();
		
		try {
			Model newModel = modelFactory.getModel("linear regression");
			ArrayList<ArrayList<Double>> dataset = new ArrayList<ArrayList<Double>>();
			newModel.train(0, "Dataset1", new ArrayList<String>(), dataset, new ArrayList<String>());
			
			System.out.println("Model catalog has " + MLSystem.getInstance().getModelCatalog().getModelNumber());
			Thread.sleep(5000);
			
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			System.out.println("Model 0 last trained at " + dateFormat.format(MLSystem.getInstance().getModelCatalog().getModel(0).getTimestamp()));
			Model retModel = modelFactory.getModel(0);
			retModel.retrain(dataset, new ArrayList<String>());
			System.out.println("Model 0 last trained at " + dateFormat.format(MLSystem.getInstance().getModelCatalog().getModel(0).getTimestamp()));
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
