package gr.ntua.ece.cslab.selis.bda.kpidb.connectors;

import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPISchema;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.Tuple;

import java.util.List;

public interface Connector {
    /*
        Method create will be used upon KPI initilization
        in order to create the corresponding table in the KPIDB
     */
    void create(KPISchema kpi_schema) throws Exception;

    /*
        Method put will be used upo KPI computation
        in order to insert the computed value in the KPIDB table
     */
    void put(KPI kpi) throws Exception;


    /*
        Used to select computed KPIs upon column filtering
     */
    List<Tuple> get(String kpi_name, Tuple filters) throws Exception;

    /*
        Used to get the n most frequent computations of a KPI
     */
    List<Tuple> getLast(String kpi_name, Integer n) throws Exception;

    /*
        Get the schema of a KPI table
     */
    KPISchema describe(String kpi_name) throws Exception;

    /*
        Get all the current KPIs
     */
    List<String> list();

    /*
        Close connection to KPI DB
     */
    void close();

}
