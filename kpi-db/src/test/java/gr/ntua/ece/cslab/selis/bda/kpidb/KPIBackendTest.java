package gr.ntua.ece.cslab.selis.bda.kpidb;

import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPISchema;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPITable;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static java.lang.System.out;
import static org.junit.Assert.*;

public class KPIBackendTest {

    public KPIBackend kpiBackend;
    @Before
    public void setUp() {
        String fs_string = "jdbc:postgresql:selis_db";
        String uname = "selis_user";
        String passwd = "123";
        kpiBackend = new KPIBackend(fs_string, uname, passwd);
        System.out.println("Connection on kpidb successfully established");
    }

    @After
    public void tearDown() {
        kpiBackend.stop();
        System.out.println("Connection on kpidb successfully closed");
    }

    public void create() {
        KPISchema kpiSchema = new KPISchema(new ArrayList<>(), new ArrayList<>());
        kpiSchema.getColumnNames().add("supplier_id");
        kpiSchema.getColumnNames().add("warehouse_id");
        kpiSchema.getColumnNames().add("blob");
        kpiSchema.getColumnTypes().add(new KeyValue("supplier_id","integer"));
        kpiSchema.getColumnTypes().add(new KeyValue("warehouse_id","integer"));
        kpiSchema.getColumnTypes().add(new KeyValue("blob","text"));
        try {
            this.kpiBackend.create(new KPITable("test_kpi", kpiSchema));
            System.out.println("Table creation completed successfully");
        }
        catch (Exception e) {
            System.out.println(e);
            System.out.println("Exception on table creation");
        }
    }

    public void insert() {
    }

    public void fetch() {
    }

    public void select() {
    }

    public void getSchema() {
    }

    public void stop() {
    }

    @Test
    public void test() {
        create();

    }
}