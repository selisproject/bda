package gr.ntua.ece.cslab.selis.bda.analytics;

class KPI {
	private int id;
	private String formula;
	private String description;
	
	
	public KPI(String formula, String description) {
		this.formula = formula;
		this.description = description;	
	}
	
	public KPI createKPI(String formula, String description, int arg) {
		return new KPI(formula, description);
	}
	
	public void calcstoreKPI(int id, String[] data) {
		double value = calculateKPI(id, data);
		
		Database database = new Database();
		database.storeKPI(id, value, data);
	}
	
	private double calculateKPI(int id, String[] data) {
		return 0;
	}

}
