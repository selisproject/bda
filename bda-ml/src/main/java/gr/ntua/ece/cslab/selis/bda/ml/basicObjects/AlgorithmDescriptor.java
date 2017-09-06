package gr.ntua.ece.cslab.selis.bda.ml.basicObjects;

import java.util.ArrayList;
import java.util.List;

public class AlgorithmDescriptor {
	private String algorithmName;
	private List<SystemDescriptor> systems;
	
	public AlgorithmDescriptor(String algorithmName, List<SystemDescriptor> systems) {
		super();
		this.algorithmName = algorithmName;
		this.systems = systems;
	}

	public String getAlgorithmName() {
		return algorithmName;
	}

	public List<SystemDescriptor> getSystems() {
		return systems;
	}

	@Override
	public String toString() {
		String retString = "Name: " + this.algorithmName + "\n";
		for (SystemDescriptor s : this.systems) {
			retString += "\tAvailable System: " + s.getSystemName() + "\n";
		}
		return retString;
	}
	
	
		
}
