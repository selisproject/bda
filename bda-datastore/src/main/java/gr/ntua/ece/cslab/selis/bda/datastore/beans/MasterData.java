package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * MasterData represents the master data to be inserted to the datastore.
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
@XmlRootElement(name = "MasterData")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class MasterData implements Serializable{
    private List<DimensionTable> tables;

    public MasterData() { this.tables = new LinkedList<>();};

    public MasterData(List<DimensionTable> tables) {
        this.tables = tables;
    }

    public List<DimensionTable> getTables() {
        return tables;
    }

    public void setTables(List<DimensionTable> tables) {
        this.tables = tables;
    }
}
