package gr.ntua.ece.cslab.selis.bda.datastore;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.DimensionTable;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.DimensionTableSchema;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MasterData;
import org.apache.hadoop.mapred.Master;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class KPIBackendTest {
    KPIBackend kpiDB;

    private List<String> createNames(String namePrefix, int nameCounter) {
        List<String> theNames = new LinkedList<>();
        for (int i = 1; i <= nameCounter; i++) {
            theNames.add(namePrefix + i);
        }
        return theNames;
    }

    private List<KeyValue> createTypes(String namePrefix, int upTo, int nameCounter) {
        List<KeyValue> theTypes = new LinkedList<>();
        for (int i = 0; i < upTo; i++) {
            theTypes.add(new KeyValue(namePrefix + i, "integer"));
        }
        for (int i = upTo; i <= nameCounter; i++) {
            theTypes.add(new KeyValue(namePrefix + i, "char(10)"));
        }
        return theTypes;
    }

    @Before
    public void setUp() throws Exception {
        String fs_string = "jdbc:postgresql:selis_db";
        String uname = "selis_user";
        String passwd = "123";

        DimensionTableSchema schema1 = new DimensionTableSchema(
                createNames("kpiA", 7),
                createTypes("kpiA", 3, 7),
                "kpiA1");

        DimensionTableSchema schema2 = new DimensionTableSchema(
                createNames("kpiB", 10),
                createTypes("kpiB", 5, 10),
                "kpiB1");

        DimensionTable kpiA = new DimensionTable("kpiA", schema1, new LinkedList<>());
        DimensionTable kpiB = new DimensionTable("kpiB", schema2, new LinkedList<>());

        List<DimensionTable> dTables = new LinkedList<>();
        dTables.add(kpiA);
        dTables.add(kpiB);

        kpiDB = new KPIBackend(fs_string, uname, passwd);
        System.out.println("Connection with KPIDB established");

        kpiDB.init(new MasterData(dTables));
        System.out.println("KPI Tables successfully created");

    }

    @After
    public void tearDown() throws Exception {
        kpiDB.stop();
        System.out.println("Closing connection to KPIDB.");
    }

    @Test
    public void KPIDBTest() throws Exception {
    }

}