package gr.ntua.ece.cslab.selis.bda.ml.models;

import java.util.ArrayList;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.ModelDescriptor;

public class GenericModel implements Model {

	private int modelID;
	private ModelDescriptor modelInfo;
	
	public GenericModel() {
		super();
		// TODO Auto-generated constructor stub
	}

	public GenericModel(int modelID, ModelDescriptor modelInfo) {
		super();
		this.modelID = modelID;
		this.modelInfo = modelInfo;
	}

	
	public int getModelID() {
		return modelID;
	}

	public ModelDescriptor getModelInfo() {
		return modelInfo;
	}

	public void train(int trainMachine, String datasetName, ArrayList<String> features, ArrayList<ArrayList<Double>> dataset, ArrayList<String> trainParameters) {}

	public void predict(List<Double> point) {}

	public void retrain(ArrayList<ArrayList<Double>> dataset, ArrayList<String> trainParameters) {}

}
