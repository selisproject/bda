package gr.ntua.ece.cslab.selis.bda.ml.models;

import java.util.ArrayList;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.ml.basicObjects.ModelDescriptor;

public interface Model {
	
	public void train(int trainMachine, String datasetName, ArrayList<String> features, ArrayList<ArrayList<Double>> dataset, ArrayList<String> trainParameters);
	public void predict(List<Double> point);
	public void retrain(ArrayList<ArrayList<Double>> dataset, ArrayList<String> trainParameters);
	
}
