package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * This class includes information about the arguments that recipes require to be executed correctly.
 * These arguments could be dimension table related, message type related or other, unrelated arguments.
 *
 */
@XmlRootElement(name = "RecipeArguments")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class RecipeArguments implements Serializable {
    private List<String> dimension_tables;
    private List<String> message_types;
    private List<String> other_args;

    /**
     * Default constructor
     */
    public RecipeArguments() {
        this.dimension_tables = new LinkedList<>();
        this.message_types = new LinkedList<>();
        this.other_args = new LinkedList<>();
    }

    /**
     * @return List of dimension tables related to the specific object
     */
    public List<String> getDimension_tables() {
        return dimension_tables;
    }

    /**
     * @param dimension_tables dimension table names to be associated with the recipe
     */
    public void setDimension_tables(List<String> dimension_tables) {
        this.dimension_tables = dimension_tables;
    }

    /**
     * @return List of message types related to the specific object
     */
    public List<String> getMessage_types() {
        return message_types;
    }

    /**
     * @param message_types Message types to be associated with the recipe
     */
    public void setMessage_types(List<String> message_types) {
        this.message_types = message_types;
    }

    /**
     * @return List of other arguments related to the specific object
     */
    public List<String> getOther_args() {
        return other_args;
    }

    /**
     * @param other_args Arguments unrelated to dimension tables or message types to be associated with the recipe
     */
    public void setOther_args(List<String> other_args) {
        this.other_args = other_args;
    }

    /**
     * @return String representation of RecipeArgument objects
     */
    @Override
    public String toString() {
        return "RecipeArguments{" +
                "dimension_tables=" + dimension_tables +
                ", message_types=" + message_types +
                ", other_args=" + other_args +
                '}';
    }
}
