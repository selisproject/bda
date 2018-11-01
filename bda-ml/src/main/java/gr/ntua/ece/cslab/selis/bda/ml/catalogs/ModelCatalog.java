package gr.ntua.ece.cslab.selis.bda.ml.catalogs;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.DatasetDescriptor;
import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.ModelDescriptor;

public class ModelCatalog {
	private static HashMap<Integer, ModelDescriptor> models;
	private static int modelCounter;
	private static ModelCatalog modelCatalog;

	private ModelCatalog() {
		models = new HashMap<Integer, ModelDescriptor>();
		modelCounter = 0;
	}
	
	public static ModelCatalog getInstance() {
		if (modelCatalog == null)
				modelCatalog = new ModelCatalog();
		return modelCatalog;
	}
	
	public ModelDescriptor getModel(int modelID) {
		return models.get(modelID);
	}
	
	public String getAllModels() {
		if (models== null)
			return "";
		else
			return models.toString();
	}
	
	public int getModelNumber() {
		return modelCounter;
	}
	
	public void addNewModel(String algoname, int trainMachine, Date timestamp, 
			String datasetName, ArrayList<String> features, String osPath) {
		DatasetDescriptor data = new DatasetDescriptor(datasetName, features);
		ModelDescriptor newModel = new ModelDescriptor(algoname, trainMachine, timestamp, data, osPath);
		models.put(modelCounter, newModel);
		modelCounter++;
	}
	
	public void updateModel(int modelID, Date timestamp) {
		models.get(modelID).setTimestamp(timestamp);
	}
}
