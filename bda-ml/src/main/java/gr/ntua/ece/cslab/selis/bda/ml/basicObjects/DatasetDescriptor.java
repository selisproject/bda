package gr.ntua.ece.cslab.selis.bda.ml.basicObjects;

import java.util.ArrayList;

public class DatasetDescriptor {
	private String datasetName;
	private ArrayList<String> features;
	
	public DatasetDescriptor(String datasetName, ArrayList<String> features) {
		super();
		this.datasetName = datasetName;
		this.features = features;
	}

	public String getDatasetName() {
		return datasetName;
	}


	public ArrayList<String> getFeatures() {
		return features;
	}
	
	
}
