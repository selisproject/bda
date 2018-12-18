package gr.ntua.ece.cslab.selis.bda.controller;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class PublicClientBackend {

    private static PublicClientBackend publicClientBackend;
    private static String authServerUrl = "";
    private static String realm = "";
    private static String client = "";

    private PublicClientBackend () {}

    public static void init(String authServerUrl_, String realm_, String client_) {
        authServerUrl = authServerUrl_;
        realm = realm_;
        client = client_;
    }

    public static PublicClientBackend getInstance() {
        if (publicClientBackend == null) {
            publicClientBackend = new PublicClientBackend();
        }

        return publicClientBackend;
    }

    public String getAccessToken(String username, String password) {
        String POST_URL = authServerUrl + "/realms/" + realm + "/protocol/openid-connect/token";

        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(POST_URL);

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("client_id", client));
        params.add(new BasicNameValuePair("username", username));
        params.add(new BasicNameValuePair("password", password));
        params.add(new BasicNameValuePair("grant_type", "password"));

        try {
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
        }

        //Execute and get the response.
        HttpResponse response = null;
        try {
            response = httpclient.execute(httppost);
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }

        String results = null;
        try {
            results = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }

        JSONObject responseObject = new JSONObject(results);

        System.out.println(responseObject);

        return (String) responseObject.get("access_token");
    }


    public List<String> getRoles(String userId, String authorizationToken) {
        String POST_URL = authServerUrl + "/admin/realms/" + realm + "/users/" + userId + "/role-mappings";

        HttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(POST_URL);
        httpGet.setHeader("Content-Type", "application/json");
        httpGet.setHeader("Authorization", "bearer " + authorizationToken);


        //Execute and get the response.
        HttpResponse response = null;
        try {
            response = httpclient.execute(httpGet);
        } catch (IOException e) {
            e.printStackTrace();

        }

        String results = null;
        try {
            results = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();

        }

        JSONObject responseObject = new JSONObject(results);

        JSONArray realmMappings = (JSONArray) responseObject.get("realmMappings");

        List<String> roles = new ArrayList<>();
        for (Object obj : realmMappings) {
            JSONObject asJsonObj = (JSONObject) obj;
            roles.add((String) asJsonObj.get("name"));
        }

        return roles;

    }

}
