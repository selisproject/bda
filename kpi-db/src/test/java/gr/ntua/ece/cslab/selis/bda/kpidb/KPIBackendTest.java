package gr.ntua.ece.cslab.selis.bda.kpidb;

import gr.ntua.ece.cslab.selis.bda.kpidb.beans.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.out;
import static java.lang.Thread.sleep;
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
        for (int i = 0; i < 10; i++) {
            int supplier;
            int warehouse;
            if (i < 5) {
                supplier = 1;
            }
            else {
                supplier = 2;
            }
            warehouse = i;
            List<KeyValue> entries = new ArrayList<>();
            entries.add(new KeyValue("supplier_id", String.valueOf(supplier)));
            entries.add(new KeyValue("warehouse_id", String.valueOf(warehouse)));
            entries.add(new KeyValue("blob", "This is a blob!"));
            KPI kpi = new KPI("test_kpi", (new Timestamp(System.currentTimeMillis())).toString(), entries);
            try {
                this.kpiBackend.insert(kpi);
                System.out.println("Insertion of a kpi completed successfully");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void fetch() {
        try {
            List<Tuple> tuples = this.kpiBackend.fetch("test_kpi", "rows", 3);
            System.out.println("Successfully fetched last "+tuples.size()+" rows");
            for (Tuple t : tuples) {
                System.out.println(t.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void select() {
    }

    public KPITable getSchema() {
        KPITable table = null;
        try {
            table = this.kpiBackend.getSchema("test_kpi");
            System.out.println("Fetch table schema completed successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return table;
    }

    @Test
    public void test() {
        create();

        System.out.println(getSchema().toString());

        insert();

        fetch();
    }
}