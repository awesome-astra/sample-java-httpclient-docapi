package com.datastax.astra;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class shows how to connect to the REST API.
 * 
 * https://astra.datastax.com
 */
public class AstraDocApiHttpClient {
    
    static final String ASTRA_TOKEN       = "change_me";
    static final String ASTRA_DB_ID       = "change_me";
    static final String ASTRA_DB_REGION   = "change_me";
    static final String ASTRA_DB_KEYSPACE = "change_me";
     
    static  Logger logger = LoggerFactory.getLogger(AstraDocApiHttpClient.class);
    
    public static void main(String[] args) throws Exception {
        
        String apiRestEndpoint = new StringBuilder("https://")
                .append(ASTRA_DB_ID).append("-")
                .append(ASTRA_DB_REGION)
                .append(".apps.astra.datastax.com/api/rest")
                .toString();
        logger.info("Document Api Endpoint is {}", apiRestEndpoint);
        
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            listNamespaces(httpClient, apiRestEndpoint);
        }
        logger.info("[OK] Success");
        System.exit(0);
    }
    
    /**
     * List Keyspaces.
     * 
     * @param httpClient
     *      current client
     * @param apiRestEndpoint
     *      api rest endpoint
     * @throws Exception 
     *      parsing or access errors
     */
    private static void listNamespaces(CloseableHttpClient httpClient, String apiRestEndpoint) throws Exception {
        // Build Request
        HttpGet listKeyspacesReq = new HttpGet(apiRestEndpoint + "/v2/schemas/namespaces");
        listKeyspacesReq.addHeader("X-Cassandra-Token", ASTRA_TOKEN);
        // Execute Request
        try(CloseableHttpResponse res = httpClient.execute(listKeyspacesReq)) {
            if (200 == res.getCode()) {
                logger.info("[OK] Namespaces list retrieved");
                logger.info("Returned message: {}", EntityUtils.toString(res.getEntity()));
                
            }
        }
    }
}
