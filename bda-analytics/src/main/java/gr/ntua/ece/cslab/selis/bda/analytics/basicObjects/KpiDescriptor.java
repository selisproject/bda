package gr.ntua.ece.cslab.selis.bda.analytics.basicObjects;
/**
 * @author nchalv
 * This class implements the descriptor of a single KPI. It involves a textual description, the KPI Primitive 
 * from which it derives, and a list of the arguments it takes.
 */

import java.util.List;

import com.google.gson.Gson;


public class KpiDescriptor {
	private String name;
	private String description;
	private Executable executable;

	public KpiDescriptor() {
	}

	public KpiDescriptor(String name, String description, Executable executable) {
		this.name = name;
		this.description = description;
		this.executable = executable;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Executable getExecutable() {
		return executable;
	}

	public void setExecutable(Executable executable) {
		this.executable = executable;
	}
}
