package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;

import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;

public class Kpi implements Runnable {

	private KpiDescriptor kpiInfo;

	public Kpi() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Kpi(int kpiID, KpiDescriptor kpiInfo) {
		super();
		this.kpiInfo = kpiInfo;
	}

	public KpiDescriptor getKpiInfo() {
		return kpiInfo;
	}

	
	public void run() {
        try {
/*                      String result = "";
                // System.out.println("Inside");
                Runtime r = Runtime.getRuntime();
                String command = kpiInfo.getExecutable().getExecutEngine().getExecutionPreamble() + " "
                                + kpiInfo.getExecutable().getOsPath();
                Process p;
                // System.out.println("Inside");

                p = r.exec(command);
                //p = r.exec("echo \"hi\"");
                p.waitFor();
                // System.out.println("Dead");

                BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                        System.out.println(inputLine);
                        result += inputLine;
                }
                in.close();*/
                List<String> cmd = new ArrayList<String>(getKpiInfo().getArguments());
                cmd.add(0, kpiInfo.getExecutable().getExecutEngine().getExecutionPreamble());
                cmd.add(kpiInfo.getExecutable().getOsPath());
                ProcessBuilder pb = new ProcessBuilder(cmd);
                //pb.directory(new File("/home/hduser/nchalv/"));
                //System.out.println(pb.directory());
                File err = new File("err");
                File out = new File("out");
                pb.redirectError(Redirect.appendTo(err));
                pb.redirectOutput(Redirect.appendTo(out));
                Process p = pb.start();
                //assert pb.redirectInput() == Redirect.PIPE;
                //assert pb.redirectOutput().file() == log;
                //assert p.getInputStream().read() == -1;
                p.waitFor();
        } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
        }

	}

	public void store(int value) {
		// TODO Auto-generated method stub

	}

}
