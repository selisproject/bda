package gr.ntua.ece.cslab.selis.bda.controller.beans;

/**
 * Created by Giannis Giannakopoulos on 10/5/17.
 * RequestResponse is used to inform the caller about the status of their request
 */
public class RequestResponse {
    private String status;
    private String details;

    /**
     * Default constructor
     */
    public RequestResponse() {
    }

    /**
     * Constructor that initializes the response elements
     * @param status
     * @param details
     */
    public RequestResponse(String status, String details) {
        this.status = status;
        this.details = details;
    }

    /**
     * Getter for the status of the request
     * @return
     */
    public String getStatus() {
        return status;
    }

    /**
     * Setter for the status
     * @param status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Getter for the details
     * @return
     */
    public String getDetails() {
        return details;
    }

    /**
     * Setter for the details
     * @param details
     */
    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public String toString() {
        return String.format("STATUS: %s, DETAILS: %s", this.status, this.details);
    }
}
