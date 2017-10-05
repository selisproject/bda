package gr.ntua.ece.cslab.selis.bda.analytics;


import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiPrimitiveCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;

public interface AnalyticsInterface {
	
	public KpiPrimitiveCatalog getKpiPrimitiveCatalog();
	public KpiCatalog getKpiCatalog();
	public KpiFactory getKpiFactory();
	
}