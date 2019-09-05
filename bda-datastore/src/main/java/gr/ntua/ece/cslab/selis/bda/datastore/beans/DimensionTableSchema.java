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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Giannis Giannakopoulos on 10/11/17.
 * This class represents the schema of a Dimension (Entity) Table.
 *
 * Entity Tables are stored in a RDBMS integrated with the BDA, therefore need to have their schema well defined according
 * to SQL requirements. The minimum information required for the initialization of such resources is the column names and types
 * as well as primary keys.
 *
 */
@XmlRootElement(name = "DimensionTableSchema")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class DimensionTableSchema {
    private List<String> columnNames;
    private List<KeyValue> columnTypes;
    private String primaryKey;

    public DimensionTableSchema(){ this.columnNames = new LinkedList<>();};

    /**
     * @param columnNames a list including the names of the table columns
     * @param columnTypes a {@link KeyValue} list including the name and type of each table column
     * @param primaryKey the table primary key
     */
    public DimensionTableSchema(List<String> columnNames, List<KeyValue> columnTypes, String primaryKey) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.primaryKey = primaryKey;
    }

    /**
     * @return a list including the names of the table columns
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * @param columnNames a list including the names of the table columns
     */
    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * @return a {@link KeyValue}  list including the name and type of each table column
     */
    public List<KeyValue> getColumnTypes() {
        return columnTypes;
    }

    /**
     * @param columnTypes a {@link KeyValue} list including the name and type of each table column
     */
    public void setColumnTypes(List<KeyValue> columnTypes) {
        this.columnTypes = columnTypes;
    }

    /**
     * @return the table primary key
     */
    public String getPrimaryKey() {
        return primaryKey;
    }

    /**
     * @param primaryKey the table primary key
     */
    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }
}
