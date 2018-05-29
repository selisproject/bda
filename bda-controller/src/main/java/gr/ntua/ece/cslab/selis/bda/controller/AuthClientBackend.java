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
