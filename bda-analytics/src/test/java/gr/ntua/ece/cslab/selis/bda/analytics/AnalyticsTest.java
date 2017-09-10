package gr.ntua.ece.cslab.selis.bda.analytics;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiPrimitiveCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.Kpi;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;

public class AnalyticsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		AnalyticsSystem mySystem = AnalyticsSystem.getInstance();
		KpiPrimitiveCatalog kpiPrimitiveCatalog = mySystem.getKpiPrimitiveCatalog();
		KpiCatalog kpiCatalog = mySystem.getKpiCatalog();

		String kpiPrimitives = kpiPrimitiveCatalog.getAllKpiPrimitives();
		assert(kpiPrimitives.equals("{}"));
		System.out.println("KPI primitives: " + kpiPrimitives);
		KpiFactory kpiFactory = mySystem.getKpiFactory();

		try {
			List<String> argtypes = Arrays.asList("argtype1", "argtype2", "argtype3");
			kpiPrimitiveCatalog.addNewKpiPrimitive(argtypes, 1, "/home/selis/blah...", "This calculates shit...");
			kpiPrimitives = kpiPrimitiveCatalog.getAllKpiPrimitives();

			System.out.println("KPI primitives: " + kpiPrimitives);
			assert(!kpiPrimitives.equals("{}"));

			// add new kpi
			int newKpiID = mySystem.getKpiCatalog().getKpiCounter();
			assert(newKpiID==0);
			int kpiPrimitiveID = mySystem.getKpiPrimitiveCatalog().getKpiPrimitiveCounter();
			assert(kpiPrimitiveID==1);

			kpiPrimitiveID = 0;
			List<String> arguments = Arrays.asList("trucks", "amount of shit");
			String description = "This calculates shit done by blue trucks...";
			Kpi newKpi = kpiFactory.getKpiByPrim(newKpiID, kpiPrimitiveID, arguments, description);

			String kpis = kpiCatalog.getAllKpis();
			assert(kpis.equals("{}"));

			System.out.println("KPIs: " + kpis);
			kpiCatalog.addNewKpi(arguments, description, newKpi.getKpiInfo().getKpiPrimitiveDescriptor());
			kpis = kpiCatalog.getAllKpis();
			assert(!kpis.equals("{}"));

			System.out.println("KPIs: " + kpis);

			/*
			 * ArrayList<ArrayList<Double>> dataset = new ArrayList<ArrayList<Double>>();
			 * newModel.train(0, "Dataset1", new ArrayList<String>(), dataset, new
			 * ArrayList<String>());
			 * 
			 * System.out.println("Model catalog has " +
			 * MLSystem.getInstance().getModelCatalog().getModelNumber());
			 * Thread.sleep(5000);
			 * 
			 * DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			 * System.out.println("Model 0 last trained at " +
			 * dateFormat.format(MLSystem.getInstance().getModelCatalog().getModel(0).
			 * getTimestamp())); Model retModel = modelFactory.getModel(0);
			 * retModel.retrain(dataset, new ArrayList<String>());
			 * System.out.println("Model 0 last trained at " +
			 * dateFormat.format(MLSystem.getInstance().getModelCatalog().getModel(0).
			 * getTimestamp()));
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
