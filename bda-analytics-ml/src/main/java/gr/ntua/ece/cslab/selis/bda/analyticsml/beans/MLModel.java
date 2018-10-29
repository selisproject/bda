package gr.ntua.ece.cslab.selis.bda.analyticsml.beans;

import java.util.Date;

public class MLModel {
	private int recipeId;
	private Date timestamp;
	private String osPath;
	
	public MLModel(int recipeId, Date timestamp, String ospath) {
		super();
		this.recipeId = recipeId;
		this.timestamp = timestamp;
		this.osPath = ospath;
	}

    public int getRecipeId() { return recipeId; }

    public void setRecipeId(int recipeId) {
        this.recipeId = recipeId;
    }

    public Date getTimestamp() { return timestamp; }

	public void setTimestamp(Date timestamp) {
	    this.timestamp = timestamp;
	}

    public String getOsPath() {
        return osPath;
    }

    public void setOsPath(String osPath) {
        this.osPath = osPath;
    }

}
