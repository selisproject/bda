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

package gr.ntua.ece.cslab.selis.bda.controller;

import java.util.Map;
import java.util.HashMap;

import org.keycloak.authorization.client.AuthzClient;
import org.keycloak.authorization.client.Configuration;

public class AuthClientBackend {
    private static AuthClientBackend authClientBackend = null;
    private static Configuration configuration = null;
    public AuthzClient authzClient = null;

    private AuthClientBackend() {
        authzClient = AuthzClient.create(configuration);
    }

    public static void init(String authServerUrl, String realm, String clientId, String secret) {
        Map<String, Object> credentials = new HashMap<String, Object>();

        credentials.put("secret", secret);

        configuration = new Configuration(
            authServerUrl, realm, clientId, credentials, null
        );
    }

    public static AuthClientBackend getInstance() {
        if (authClientBackend == null) {
            authClientBackend = new AuthClientBackend();
        }

        return authClientBackend;
    }
}
