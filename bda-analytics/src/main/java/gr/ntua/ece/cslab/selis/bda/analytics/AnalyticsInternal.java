package gr.ntua.ece.cslab.selis.bda.analytics;

import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutableCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;

public class AnalyticsInternal {

    private KPIBackend kpidb;

    public AnalyticsInternal() {

    }

    public ExecutableCatalog getExecutableCatalog() {
        // TODO Auto-generated method stub
        return ExecutableCatalog.getInstance();
    }

    public KpiCatalog getKpiCatalog() {
        // TODO Auto-generated method stub
        return KpiCatalog.getInstance();
    }

    public KpiFactory getKpiFactory() {
        // TODO Auto-generated method stub
        return KpiFactory.getInstance();
    }
}
