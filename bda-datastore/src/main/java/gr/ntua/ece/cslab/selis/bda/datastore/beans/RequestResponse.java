/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.selis.bda.datastore.beans;

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
