package gr.ntua.ece.cslab.selis.bda.analytics.kpis;

public class OrderForecast {
    private String date;
    private int quantity;
    private String sku;

    public OrderForecast() {
    }

    public OrderForecast(String date, int quantity, String sku) {
        this.date = date;
        this.quantity = quantity;
        this.sku = sku;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }
}
