package gr.ntua.ece.cslab.selis.bda.ml.models;


import java.util.List;
import java.util.ArrayList;
import java.util.Date;

import gr.ntua.ece.cslab.selis.bda.ml.MLSystem;
import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.DatasetDescriptor;
import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.ModelDescriptor;


public class LinearRegression extends GenericModel {

	
	public LinearRegression(int modelID, ModelDescriptor model) {
		super(modelID, model);
	}

	public LinearRegression() {
		super();
	}

	@Override
	public void train(int trainMachine, String datasetName, ArrayList<String> features, ArrayList<ArrayList<Double>> dataset, ArrayList<String> trainParameters) {
		
		System.out.println("Linear Regression train done");
		MLSystem.getInstance().getModelCatalog().addNewModel("linear regression", trainMachine, new Date(), datasetName, features, "");
		
	}

	@Override
	public void predict(List<Double> point) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void retrain(ArrayList<ArrayList<Double>> dataset, ArrayList<String> trainParameters) {
		System.out.println("Linear regression retrain done.");
		this.getModelInfo().setTimestamp(new Date());
		MLSystem.getInstance().getModelCatalog().updateModel(this.getModelID(), this.getModelInfo().getTimestamp());
	}
	

}
