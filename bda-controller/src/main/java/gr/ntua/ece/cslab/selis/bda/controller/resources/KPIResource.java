package gr.ntua.ece.cslab.selis.bda.controller.resources;

import com.google.common.base.Splitter;
import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KPIDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * KPIResource holds the method for the analytics module.
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
@Path("kpi")
public class KPIResource {
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<KPIDescription> getKPIList() {
        // TODO: implement the method
        return new LinkedList<>();
    }

    @POST
    @Path("{id}/run")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse runKPI(@PathParam("id") String id) {
        // TODO: implement the method
        return new RequestResponse("Done", "Double.toString()");
    }

    @GET
    @Path("{kpiname}/fetch")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Tuple> getLastKPIs(
            @PathParam("kpiname") String kpiname,
            @QueryParam("n") Integer n
    ) {
        System.out.println("Entered fetch function");
        System.out.println(kpiname + "," + n);
        try {
            return Entrypoint.kpiDB.fetch(kpiname, "rows", n);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList();
    }

    @GET
    @Path("{kpiname}/select")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Tuple> selectKPIs(
            @PathParam("kpiname") String kpiname,
            @QueryParam("filters") String filters
    ) {
        List<KeyValue> args = new LinkedList<>();
        if(filters != null && !filters.isEmpty()) {
            System.out.println("Select parameters given");
            Map<String, String> myfilters = Splitter.on(';').withKeyValueSeparator(":").split(filters);
            for (Map.Entry entry : myfilters.entrySet()) {
                args.add(new KeyValue(entry.getKey().toString(), entry.getValue().toString()));
            }
        }
        try {
            return Entrypoint.kpiDB.select(kpiname,args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList();
    }
}
