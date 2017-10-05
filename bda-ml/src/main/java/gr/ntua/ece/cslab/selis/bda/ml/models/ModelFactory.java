package gr.ntua.ece.cslab.selis.bda.ml.models;

import gr.ntua.ece.cslab.selis.bda.ml.MLSystem;
import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.ModelDescriptor;

public class ModelFactory {
	
	private static ModelFactory modelFactory;
	
	private ModelFactory() {
		super();
	}
	
	public static ModelFactory getInstance() {
		if (modelFactory == null)
				modelFactory = new ModelFactory();
		return modelFactory;
	}
	
	public Model getModel(String algoName) throws Exception {
		if (algoName.equals("linear regression")) 
			return new LinearRegression();
		else if (algoName.equals("regression ANN"))
			return new RegressionANN();
		else
			throw new Exception("No such algorithm available.");
	}
	
	public Model getModel(int modelID) throws Exception {
		ModelDescriptor model = MLSystem.getInstance().getModelCatalog().getModel(modelID);
		if (model.getAlgoName().equals("linear regression")) 
			return new LinearRegression(modelID, model);
		else if (model.getAlgoName().equals("regression ANN"))
			return new RegressionANN(modelID, model);
		else
			throw new Exception("No such algorithm available.");
	}
	
	
}
