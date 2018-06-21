package gr.ntua.ece.cslab.selis.bda.analytics;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutEngineCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.runners.RunnerFactory;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;

import java.sql.ResultSet;

public class AnalyticsInstance {

    private KPIBackend kpidb;
    private ExecutEngineCatalog engineCatalog;
    private KpiCatalog kpiCatalog;

    AnalyticsInstance(String kpidbURL, String username,
                      String password) {
        this.kpidb = new KPIBackend(kpidbURL, username, password);
        this.kpiCatalog = new KpiCatalog();
        this.engineCatalog = new ExecutEngineCatalog();
    }

    public KPIBackend getKpidb() {
        return kpidb;
    }

    public KpiCatalog getKpiCatalog() { return kpiCatalog; }

    public ExecutEngineCatalog getEngineCatalog() { return engineCatalog; }

    public void run(int recipe_id, String message){
        KpiDescriptor kpi = kpiCatalog.getKpi(recipe_id);
        ExecutEngineDescriptor engine =  engineCatalog.getExecutEngine(
                kpi.getExecutable().getEngineID()
        );
        System.out.println("Engine id : " + kpi.getExecutable().getEngineID());
        System.out.println(kpi.toString());
        System.out.println(engine);
        Runnable runner = RunnerFactory.getInstance().getRunner(kpi, engine, message, this.kpidb);


        Thread t = new Thread(runner);

        t.start();

        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



}
