package gr.ntua.ece.cslab.selis.bda.ml;

public class MLSystem implements MLInterface {

	private MLSystem system;
	
	private MLSystem() {	}
	
	public MLSystem getInstance() {
		if (system == null)
			this.system = new MLSystem();
		return system;
		
	}

	public void getAlgoCatalog() {
		// TODO Auto-generated method stu

	}

	public void getModelCatalog() {
		// TODO Auto-generated method stub

	}

	public void getModelFactory() {
		// TODO Auto-generated method stub

	}

}
