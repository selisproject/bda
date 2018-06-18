package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import org.json.JSONObject;

import java.util.Iterator;

public class ArgumentParser {
    public String get_executable_arguments(JSONObject args) {
        String arguments = "";
        Iterator<String> argNames = args.keys();
        while (argNames.hasNext()) {
            String name = argNames.next();
            arguments += "--" + name + "=" + args.get(name).toString() + " ";
        }
        return arguments;
    }
}
