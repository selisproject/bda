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

package gr.ntua.ece.cslab.selis.bda.kpidb.connectors;

import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.HBaseConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.LocalFSConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

public class KPIConnectorFactory {
    private static KPIConnectorFactory connFactory;

    private KPIConnectorFactory() {}

    public static KPIConnectorFactory getInstance(){
        if (connFactory == null)
            connFactory = new KPIConnectorFactory();
        return connFactory;
    }

    public KPIConnector generateConnector(Connector conn){


        KPIConnector connector = null;
        if (conn instanceof PostgresqlConnector) {
            connector = new KPIPostgresqlConnector( (PostgresqlConnector) conn );
        }
        return connector;
    }
}
