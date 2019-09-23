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

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;



public class AuthClientBackend {

    public static String getAccessToken() {
        Configuration.AuthClientBackend authClientBackend = Configuration.getInstance().authClientBackend;
        StringBuilder builder = new StringBuilder();
        builder.append(authClientBackend.getAuthServerUrl());
        builder.append("realms/");
        builder.append(authClientBackend.getRealm());
        builder.append("/protocol/openid-connect/token");
        String POST_URL = builder.toString();

        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(POST_URL);

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("client_id", authClientBackend.getClientId()));
        params.add(new BasicNameValuePair("username", authClientBackend.getBdaUsername()));
        params.add(new BasicNameValuePair("password", authClientBackend.getBdaPassword()));
        params.add(new BasicNameValuePair("grant_type", "password"));
        params.add(new BasicNameValuePair("client_secret", authClientBackend.getClientSecret()));

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

        //System.out.println(responseObject);

        return (String) responseObject.get("access_token");
    }

}
