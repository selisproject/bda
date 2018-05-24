package gr.ntua.ece.cslab.selis.bda.controller.resources;

import com.google.common.base.Splitter;
import com.google.gson.JsonObject;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.OrderForecast;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.SonaeKPI;
import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.Tuple;
import org.apache.avro.Schema;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mortbay.util.ajax.JSON;

import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
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
    public List<SonaeKPI> getLastKPIs(
            @PathParam("kpiname") String kpiname,
            @QueryParam("n") Integer n
    ) {
        System.out.println("Entered fetch function");
        System.out.println(kpiname + "," + n);
        List<SonaeKPI> result = new LinkedList<>();
        try {
            List<Tuple> results = Entrypoint.kpiDB.fetch(kpiname, "rows", n);
            for (Tuple tuple : results) {
                SonaeKPI row = new SonaeKPI();
                for (KeyValue cell : tuple.getTuple()) {
                    if (cell.getKey().contentEquals("timestamp")) {
                        row.setTimestamp(cell.getValue());
                    }
                    if (cell.getKey().contentEquals("supplier_id")) {
                        row.setSupplier_id(Integer.parseInt(cell.getValue()));
                    }
                    if (cell.getKey().contentEquals("warehouse_id")) {
                        row.setWarehouse_id(Integer.parseInt(cell.getValue()));
                    }
                    if (cell.getKey().contentEquals("salesforecast_id")) {
                        row.setSalesforecast_id(Integer.parseInt(cell.getValue()));
                    }
                    if (cell.getKey().contentEquals("result")) {
                        ObjectMapper mapper = new ObjectMapper();
                        List<OrderForecast> forecast = mapper.readValue(cell.getValue(), new TypeReference<List<OrderForecast>>(){});
                        row.setResult(forecast);
                    }
                }
                //System.out.println(row.toString());
                result.add(row);
            }
            //System.out.println(result);

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    @GET
    @Path("{kpiname}/select")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<SonaeKPI> selectKPIs(
            @PathParam("kpiname") String kpiname,
            @QueryParam("filters") String filters
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
        List<SonaeKPI> result = new LinkedList<>();
        try {
            List<Tuple> results = Entrypoint.kpiDB.select(kpiname,new Tuple(args));
            for (Tuple tuple : results) {
                SonaeKPI row = new SonaeKPI();
                for (KeyValue cell : tuple.getTuple()) {
                    if (cell.getKey().contentEquals("timestamp")) {
                        row.setTimestamp(cell.getValue());
                    }
                    if (cell.getKey().contentEquals("supplier_id")) {
                        row.setSupplier_id(Integer.parseInt(cell.getValue()));
                    }
                    if (cell.getKey().contentEquals("warehouse_id")) {
                        row.setWarehouse_id(Integer.parseInt(cell.getValue()));
                    }
                    if (cell.getKey().contentEquals("salesforecast_id")) {
                        row.setSalesforecast_id(Integer.parseInt(cell.getValue()));
                    }
                    if (cell.getKey().contentEquals("result")) {
                        ObjectMapper mapper = new ObjectMapper();
                        List<OrderForecast> forecast = mapper.readValue(cell.getValue(), new TypeReference<List<OrderForecast>>(){});
                        row.setResult(forecast);
                    }
                }
                //System.out.println(row.toString());
                result.add(row);
            }
            //System.out.println(result);

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList();
    }
}
