package gr.ntua.ece.cslab.selis.bda.datastore;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import org.apache.hadoop.mapred.Master;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Key;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class KPIBackendTest {
    KPIBackend kpiDB;
    private Random rn;

    private List<String> createNames(String namePrefix, int nameCounter) {
        List<String> theNames = new LinkedList<>();
        for (int i = 1; i <= nameCounter; i++) {
            theNames.add(namePrefix + i);
        }
        theNames.add("timestamp");
        return theNames;
    }

    private List<KeyValue> createTypes(String namePrefix, int upTo, int nameCounter) {
        List<KeyValue> theTypes = new LinkedList<>();
        for (int i = 1; i < upTo; i++) {
            theTypes.add(new KeyValue(namePrefix + i, "integer"));
        }
        for (int i = upTo; i <= nameCounter; i++) {
            theTypes.add(new KeyValue(namePrefix + i, "text"));
        }
        theTypes.add(new KeyValue("timestamp", "bigint"));
        return theTypes;
    }

    private List<KeyValue> createEntries(String namePrefix, int nameCounter) {
        List<KeyValue> theEntries = new LinkedList<>();
        for (int i = 1; i <= nameCounter; i++) {
            theEntries.add(new KeyValue(namePrefix + i, Integer.toString(i)));
        }
        return theEntries;
    }


    private long getRandomTimeBetweenTwoDates () {
        long beginTime = Timestamp.valueOf("2017-01-01 00:00:00").getTime();
        long endTime = Timestamp.valueOf("2013-12-31 00:58:00").getTime();
        long diff = endTime - beginTime + 1;
        return beginTime + (long) (Math.random() * diff);
    }

    private KPIDescription getSonaekpiEntry() {
        List<KeyValue> data = new LinkedList<>();
        data.add(new KeyValue("salesforecast_id", Integer.toString(rn.nextInt(10) + 1)));
        data.add(new KeyValue("supplier_id", Integer.toString(rn.nextInt(10) + 1)));
        data.add(new KeyValue("warehouse_id", Integer.toString(rn.nextInt(10) + 1)));
        data.add(new KeyValue("result", "output string"));
        return new KPIDescription("sonae_orderforecast", System.currentTimeMillis(), data);
    }

    private DimensionTable getSonaeDT() {
        List<String> sonaenames = new LinkedList<>();
        List<KeyValue> sonaetypes = new LinkedList<>();

        sonaenames.add("timestamp");
        sonaenames.add("salesforecast_id");
        sonaenames.add("supplier_id");
        sonaenames.add("warehouse_id");
        sonaenames.add("result");

        sonaetypes.add(new KeyValue("timestamp", "bigint"));
        sonaetypes.add(new KeyValue("salesforecast_id", "integer"));
        sonaetypes.add(new KeyValue("supplier_id", "integer"));
        sonaetypes.add(new KeyValue("warehouse_id", "integer"));
        sonaetypes.add(new KeyValue("result", "text"));

        DimensionTableSchema sonaeschema = new DimensionTableSchema(
                sonaenames,
                sonaetypes,
                "timestamp"
        );

        return new DimensionTable(
                "sonae_orderforecast",
                sonaeschema,
                new LinkedList<>()
        );


    }

    @Before
    public void setUp() throws Exception {
    /*
        Local db credentials
     */
        String fs_string = "jdbc:postgresql:selis_db";
        String uname = "selis_user";
        String passwd = "123";

        /*
            Remote db credentials
         */
        //String fs_string = "jdbc:postgresql://147.102.4.108:5432/sonae";
        //String fs_string = "jdbc:postgresql://10.0.1.4:5432/sonae";
        //String uname = "clms";
        //String passwd = "sonae@sEl1s";
        rn = new Random();
        DimensionTableSchema schema1 = new DimensionTableSchema(
                createNames("kpia", 7),
                createTypes("kpia", 3, 7),
                "timestamp");

        DimensionTableSchema schema2 = new DimensionTableSchema(
                createNames("kpib", 10),
                createTypes("kpib", 5, 10),
                "timestamp");



        DimensionTable kpiA = new DimensionTable("kpia", schema1, new LinkedList<>());
        DimensionTable kpiB = new DimensionTable("kpib", schema2, new LinkedList<>());

        List<DimensionTable> dTables = new LinkedList<>();
        dTables.add(kpiA);
        dTables.add(kpiB);
        dTables.add(getSonaeDT());

//        kpiDB = new KPIBackend(fs_string, uname, passwd);
        System.out.println("Connection with KPIDB established");

        //kpiDB.init(new MasterData(dTables));
        System.out.println("KPI Tables successfully created");

    }

    @After
    public void tearDown() throws Exception {
  //      kpiDB.stop();
        System.out.println("Closing connection to KPIDB.");
    }

    @Test
    public void KPIDBTest() throws Exception {
    /*
        ******** General Test Section
     */
 /*       KPIDescription kpiA1 = new KPIDescription("kpia",
                System.currentTimeMillis(),
                createEntries("kpia", 7));
        KPIDescription kpiB1 = new KPIDescription("kpib",
                System.currentTimeMillis(),
                createEntries("kpib", 10));
        kpiDB.insert(kpiA1);
        kpiDB.insert(kpiB1);

        kpiA1.getEntries().get(0).setValue("10");
        kpiB1.getEntries().get(0).setValue("10");
        TimeUnit.SECONDS.sleep(10);

        kpiA1.setTimestamp(System.currentTimeMillis());
        kpiB1.setTimestamp(System.currentTimeMillis());

        kpiDB.insert(kpiA1);
        kpiDB.insert(kpiB1);

        kpiA1.getEntries().get(1).setValue("20");
        kpiB1.getEntries().get(1).setValue("20");

        TimeUnit.SECONDS.sleep(10);

        kpiA1.setTimestamp(System.currentTimeMillis());
        kpiB1.setTimestamp(System.currentTimeMillis());

        kpiDB.insert(kpiA1);
        kpiDB.insert(kpiB1);

        System.out.println("Fetch 2 last kpis result");
        List<Tuple> result = kpiDB.fetch("kpia", "rows", 2);
        int i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }

        System.out.println("Fetch 1 last kpis result");
        result = kpiDB.fetch("kpib", "rows", 1);
        i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }

        List<KeyValue> args = new LinkedList<>();
        args.add(new KeyValue("kpia1", "10"));

        System.out.println("Select statement with 1 args");
        result = kpiDB.select("kpia", args);
        i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }

        System.out.println("Select statement with 2 args");
        args.add(new KeyValue("kpia2", "20"));
        result = kpiDB.select("kpia", args);
        i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }

        System.out.println("Select statement with 3 args");
        args.add(new KeyValue("kpia7", "7"));
        result = kpiDB.select("kpia", args);
        i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }
            */

    /*
        Sonae - specific test
     */
        /*
               Populate Database
         */
        int i;

/*
        List<KPIDescription> sonaeresults = new LinkedList<>();
        for (i = 0; i < 45; i++) {
            sonaeresults.add(getSonaekpiEntry());
            TimeUnit.SECONDS.sleep(2);
        }

        for (KPIDescription res : sonaeresults) {
            kpiDB.insert(res);
        }
*/
        /*
                Check the select statements
         */

        /*
        System.out.println("Fetch 2 last kpis result");
        List<Tuple> result = kpiDB.fetch("sonaekpi_0", "rows", 2);
        i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }

        List<KeyValue> args = new LinkedList<>();
        args.add(new KeyValue("supplierid", "10"));

        System.out.println("Select statement with 1 args");
        result = kpiDB.select("sonaekpi_0", args);
        i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }

        args.add(new KeyValue("warehouseid", "6"));

        System.out.println("Select statement with 2 args");
        result = kpiDB.select("sonaekpi_0", args);
        i = 1;
        for (Tuple tuple : result) {
            System.out.println("Tuple : " + i);
            for (KeyValue cell : tuple.getTuple()) {
                System.out.println(cell.getKey() + "," + cell.getValue());
            }
            i++;
        }
        */

    }

}