package gr.ntua.ece.cslab.selis.bda.controller.resources;

import com.google.common.base.Splitter;
import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPITable;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.Tuple;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
    public List<KPI> getKPIList() {
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
    public Response getLastKPIs(
            @PathParam("kpiname") String kpiname,
            @QueryParam("n") Integer n,
            @HeaderParam("Accept") String accepted
    ) {

        System.out.println("Entered fetch function");
        System.out.println(kpiname + "," + n);

        try {
            List<Tuple> results = Entrypoint.analyticsComponent.getKpidb().fetch(kpiname, "rows", n);
            KPITable table = Entrypoint.analyticsComponent.getKpidb().getSchema(kpiname);
            JSONArray returnResults = new JSONArray();
            for (Tuple tuple : results) {
                JSONObject row = new JSONObject();
                for (KeyValue cell : tuple.getTuple()) {
                    for (KeyValue type : table.getKpi_schema().getColumnTypes()) {
                        if (cell.getKey().equals(type.getKey())) {
                            if (type.getValue().contains("integer"))
                                row.put(cell.getKey(), Integer.valueOf(cell.getValue()));
                            else if (type.getValue().contains("bigint"))
                                row.put(cell.getKey(), Long.valueOf(cell.getValue()));
                            else if (type.getValue().contains("json"))
                                if (cell.getValue().startsWith("["))
                                    row.put(cell.getKey(), new JSONArray(cell.getValue()));
                                else
                                    row.put(cell.getKey(), new JSONObject(cell.getValue()));
                            else
                                row.put(cell.getKey(), cell.getValue());
                        }
                    }
                }
                returnResults.put(row);
            }
            System.out.println(accepted);
            System.out.println(MediaType.valueOf(accepted));
            System.out.println(returnResults.toString());
            if(accepted != null) {
                MediaType mediaType = MediaType.valueOf(accepted);
                if (mediaType.equals(MediaType.valueOf(MediaType.APPLICATION_XML)))
                    return Response.ok().entity(XML.toString(returnResults)).type(mediaType).build();
                else if (mediaType.equals(MediaType.valueOf(MediaType.APPLICATION_JSON)))
                    return Response.ok().entity(returnResults.toString()).type(mediaType).build();
            }
            // service logic
            return Response.ok().entity(XML.toString(returnResults)).type(MediaType.APPLICATION_XML).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.noContent().build();
    }

    @GET
    @Path("{kpiname}/select")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response selectKPIs(
            @PathParam("kpiname") String kpiname,
            @QueryParam("filters") String filters,
            @HeaderParam("Accept") String accepted
    ) {
        List<KeyValue> args = new LinkedList<>();
        if(filters != null && !filters.isEmpty()) {
            System.out.println("Select parameters given");
            Map<String, String> myfilters = Splitter.on(';').withKeyValueSeparator(":").split(filters);
            for (Map.Entry entry : myfilters.entrySet()) {
                //System.out.println(entry.getKey() + " , " + entry.getValue());
                args.add(new KeyValue(entry.getKey().toString(), entry.getValue().toString()));
            }
        }
        try {
            List<Tuple> results = Entrypoint.analyticsComponent.getKpidb().select(kpiname,new Tuple(args));
            KPITable table = Entrypoint.analyticsComponent.getKpidb().getSchema(kpiname);
            JSONArray returnResults = new JSONArray();
            for (Tuple tuple : results) {
                JSONObject row = new JSONObject();
                for (KeyValue cell : tuple.getTuple()) {
                    for (KeyValue type : table.getKpi_schema().getColumnTypes()) {
                        if (cell.getKey().equals(type.getKey())) {
                            if (type.getValue().contains("integer"))
                                row.put(cell.getKey(), Integer.valueOf(cell.getValue()));
                            else if (type.getValue().contains("bigint"))
                                row.put(cell.getKey(), Long.valueOf(cell.getValue()));
                            else if (type.getValue().contains("json"))
                                if (cell.getValue().startsWith("["))
                                    row.put(cell.getKey(), new JSONArray(cell.getValue()));
                                else
                                    row.put(cell.getKey(), new JSONObject(cell.getValue()));
                            else
                                row.put(cell.getKey(), cell.getValue());
                        }
                    }
                }
                returnResults.put(row);
            }
            System.out.println(accepted);
            System.out.println(MediaType.valueOf(accepted));
            System.out.println(returnResults.toString());
            if(accepted != null) {
                MediaType mediaType = MediaType.valueOf(accepted);
                if (mediaType.equals(MediaType.valueOf(MediaType.APPLICATION_XML)))
                    return Response.ok().entity(XML.toString(returnResults)).type(mediaType).build();
                else if (mediaType.equals(MediaType.valueOf(MediaType.APPLICATION_JSON)))
                    return Response.ok().entity(returnResults.toString()).type(mediaType).build();
            }
            // service logic
            return Response.ok().entity(XML.toString(returnResults)).type(MediaType.APPLICATION_XML).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.noContent().build();
    }
}
