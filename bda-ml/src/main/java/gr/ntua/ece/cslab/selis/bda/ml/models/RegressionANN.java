package gr.ntua.ece.cslab.selis.bda.ml.models;

import java.util.List;
import java.util.ArrayList;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.ModelDescriptor;

public class RegressionANN extends GenericModel{

	public RegressionANN(int modelID, ModelDescriptor model) {
		super(modelID, model);
	}

	public RegressionANN() {
		super();
	}

	@Override
	public void train(int trainMachine, String datasetName, ArrayList<String> features, ArrayList<ArrayList<Double>> dataset, ArrayList<String> trainParameters) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void predict(List<Double> point) {
		// TODO Auto-generated method stub
		
	}
	
	

}
