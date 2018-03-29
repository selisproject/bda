package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class SonaeKPI {
    private String timestamp;
    private String salesforecast_id;
    private String supplier_id;
    private String warehouse_id;
    private String result;

    public SonaeKPI() {
    }

    public SonaeKPI(String timestamp, String salesforecast_id, String supplier_id, String warehouse_id, String result) {
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

    public String getSalesforecast_id() {
        return salesforecast_id;
    }

    public void setSalesforecast_id(String salesforecast_id) {
        this.salesforecast_id = salesforecast_id;
    }

    public String getSupplier_id() {
        return supplier_id;
    }

    public void setSupplier_id(String supplier_id) {
        this.supplier_id = supplier_id;
    }

    public String getWarehouse_id() {
        return warehouse_id;
    }

    public void setWarehouse_id(String warehouse_id) {
        this.warehouse_id = warehouse_id;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
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
