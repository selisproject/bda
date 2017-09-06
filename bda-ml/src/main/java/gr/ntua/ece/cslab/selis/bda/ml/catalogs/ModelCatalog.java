package gr.ntua.ece.cslab.selis.bda.ml.catalogs;

import java.util.HashMap;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.ModelDescriptor;

public class ModelCatalog {
	private HashMap<Integer, ModelDescriptor> models;
	private int modelCounter;
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
	
	public String getAllModels() {
		if (models== null)
			return "";
		else
			return models.toString();
	}
	
	public void addNewModel() {
		
	}
}
