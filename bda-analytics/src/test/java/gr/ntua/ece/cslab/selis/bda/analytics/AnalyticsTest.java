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
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutEngineCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutableCatalog;
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
		ExecutEngineCatalog executEngineCatalog = ExecutEngineCatalog.getInstance();
		ExecutableCatalog executableCatalog = mySystem.getExecutableCatalog();
		KpiCatalog kpiCatalog = mySystem.getKpiCatalog();

		String executables = executableCatalog.getAllExecutables();
		assert (executables.equals("{}"));
		System.out.println("KPI primitives: " + executables);
		KpiFactory kpiFactory = mySystem.getKpiFactory();

		try {
			executEngineCatalog.addNewExecutEngine("python3", "python3");
			System.out.println(executEngineCatalog.getAllExecutEngines());
			List<String> argtypes = Arrays.asList();
			executableCatalog.addNewExecutable(argtypes, executEngineCatalog.getExecutEngine(0), "./bin/kpi_null.py",
					"This calculates 0");
			executables = executableCatalog.getAllExecutables();

			System.out.println("KPI binaries: " + executables);
			assert (!executables.equals("{}"));

			// add new kpi
			int newKpiID = mySystem.getKpiCatalog().getKpiCounter();
			assert (newKpiID == 0);
			int executableID = mySystem.getExecutableCatalog().getExecutableCounter();
			assert (executableID == 1);

			executableID = 0;
			List<String> arguments = Arrays.asList("trucks", "amount of shit");
			String description = "This calculates shit done by blue trucks...";
			Kpi newKpi = kpiFactory.getKpiByExecutable(newKpiID, executableID, arguments, description);

			String kpis = kpiCatalog.getAllKpis();
			assert (kpis.equals("{}"));

			System.out.println("KPIs: " + kpis);
			kpiCatalog.addNewKpi(arguments, description, newKpi.getKpiInfo().getExecutable());
			kpis = kpiCatalog.getAllKpis();
			assert (!kpis.equals("{}"));
			Kpi kpi = kpiFactory.getKpiById(0);

			System.out.println("KPIs: " + kpis);
			// kpi.calculate();
			(new Thread(kpi)).start();
			try {
				Thread.sleep(3000);
				// System.out.println("Out");

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// System.out.println("Output: "+kpi.calculate());

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
