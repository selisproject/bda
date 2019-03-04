package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import java.util.*;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;

public class LocalRunner extends ArgumentParser implements Runnable {

    String messageId;
    String scnSlug;
    Recipe recipe;
    ExecutionEngine engine;

    public LocalRunner(Recipe recipe,
                    ExecutionEngine engine,
                    String messageId,
                    String SCNslug) {
        this.recipe = recipe;
        this.engine = engine;
        this.messageId = messageId;
        this.scnSlug = SCNslug;

    }

    public void run() {
        try {
            if (engine.getArgs().length() != 0) {
                // TODO: Add code to support engine arguments
            }

            ProcessBuilder pb = new ProcessBuilder(Arrays.asList(
                    engine.getEngine_path(), recipe.getExecutablePath(), messageId));

            //File out = new File("/results/" + recipe.getName() + ".out");
            //pb.redirectError(ProcessBuilder.Redirect.to(out));
            //pb.redirectOutput(ProcessBuilder.Redirect.to(out));
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process p = pb.start();
            p.waitFor();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
