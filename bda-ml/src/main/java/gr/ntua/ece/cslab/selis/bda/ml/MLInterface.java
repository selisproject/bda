package gr.ntua.ece.cslab.selis.bda.ml;

import gr.ntua.ece.cslab.selis.bda.ml.catalogs.AlgorithmCatalog;
import gr.ntua.ece.cslab.selis.bda.ml.catalogs.ModelCatalog;

public interface MLInterface {
	
	public AlgorithmCatalog getAlgoCatalog();
	public ModelCatalog getModelCatalog();
	public void getModelFactory();
	
}
