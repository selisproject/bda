package gr.ntua.ece.cslab.selis.bda.analytics;


import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutableCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;

public interface AnalyticsInterface {
	
	public ExecutableCatalog getExecutableCatalog();
	public KpiCatalog getKpiCatalog();
	public KpiFactory getKpiFactory();
	
}