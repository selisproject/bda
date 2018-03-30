package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class SonaeKPI {
    private String timestamp;
    private int salesforecast_id;
    private int supplier_id;
    private int warehouse_id;
    private List<OrderForecast> result;

    public SonaeKPI() {
    }

    public SonaeKPI(String timestamp, int salesforecast_id, int supplier_id, int warehouse_id, List<OrderForecast> result) {
        this.timestamp = timestamp;
        this.salesforecast_id = salesforecast_id;
        this.supplier_id = supplier_id;
        this.warehouse_id = warehouse_id;
        this.result = result;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getSalesforecast_id() {
        return salesforecast_id;
    }

    public void setSalesforecast_id(int salesforecast_id) {
        this.salesforecast_id = salesforecast_id;
    }

    public int getSupplier_id() {
        return supplier_id;
    }

    public void setSupplier_id(int supplier_id) {
        this.supplier_id = supplier_id;
    }

    public int getWarehouse_id() {
        return warehouse_id;
    }

    public void setWarehouse_id(int warehouse_id) {
        this.warehouse_id = warehouse_id;
    }

    public List<OrderForecast> getResult() {
        return result;
    }

    public void setResult(List<OrderForecast> result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "SonaeKPI{" +
                "timestamp=" + timestamp +
                ", salesforecast_id=" + salesforecast_id +
                ", supplier_id=" + supplier_id +
                ", warehouse_id=" + warehouse_id +
                ", result='" + result + '\'' +
                '}';
    }
}
