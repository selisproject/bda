package gr.ntua.ece.cslab.selis.bda.analytics.basicObjects;
/**
 * @author nchalv
 * This class implements the descriptor of a single KPI. It involves a textual description, the KPI Primitive 
 * from which it derives, and a list of the arguments it takes.
 */

import java.util.List;

import com.google.gson.Gson;


public class KpiDescriptor {
	private String description;
	private Executable executable;
	private List<String> eng_arguments;
	private List<String> arguments;
	
	public KpiDescriptor(String description, Executable executable, List<String> eng_arguments, List<String> arguments) {
		this.description = description;
		this.executable = executable;
		this.arguments = arguments;
		this.eng_arguments = eng_arguments;

	}


	public List<String> getEng_arguments() {
		return eng_arguments;
	}


	public String getDescription() {
		return description;
	}


	public Executable getExecutable() {
		return executable;
	}


	public List<String> getArguments() {
		return arguments;
	}
	
	
	public String toJson() {
		return new Gson().toJson(this);
	}

}
