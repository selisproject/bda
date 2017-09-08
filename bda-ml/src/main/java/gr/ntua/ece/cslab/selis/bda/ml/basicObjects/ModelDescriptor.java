package gr.ntua.ece.cslab.selis.bda.ml.basicObjects;

import java.util.Date;

public class ModelDescriptor {
	private String algoName;
	private int trainMachine;
	private Date timestamp;
	private DatasetDescriptor datasetDescriptor;
	private String osPath;
	
	public ModelDescriptor(String algoName, int trainMachine, Date timestamp, DatasetDescriptor datasetDescriptor, String ospath) {
		super();
		this.algoName = algoName;
		this.trainMachine = trainMachine;
		this.timestamp = timestamp;
		this.datasetDescriptor = datasetDescriptor;
		this.osPath = ospath;
	}

	public String getAlgoName() {
		return algoName;
	}

	public int getTrainMachine() {
		return trainMachine;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public DatasetDescriptor getDatasetDescriptor() {
		return datasetDescriptor;
	}
	
	public String getOsPath() {
		return algoName;
	}

}
