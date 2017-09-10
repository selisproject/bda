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
	private KpiPrimitiveDescriptor kpiPrimitiveDescriptor;
	private List<String> arguments;
	
	
	public KpiDescriptor(String description, KpiPrimitiveDescriptor kpiPrimitiveDescriptor, List<String> arguments) {
		this.description = description;
		this.kpiPrimitiveDescriptor = kpiPrimitiveDescriptor;
		this.arguments = arguments;
	}


	public String getDescription() {
		return description;
	}


	public KpiPrimitiveDescriptor getKpiPrimitiveDescriptor() {
		return kpiPrimitiveDescriptor;
	}


	public List<String> getArguments() {
		return arguments;
	}
	
	
	public String toJson() {
		return new Gson().toJson(this);
	}

}
