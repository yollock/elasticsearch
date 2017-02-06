package org.elasticsearch.a;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class EsUtils {
    public static final String INDEX_NAME = "yoll_index";
    public static final String TYPE_NAME = "yoll_type";

    private static TransportClient client;

    public static Client getEsClient() {
        Map<String, String> settingConfig = new HashMap<String, String>();
        settingConfig.put("client.transport.sniff", "true");
        settingConfig.put("cluster.name", "elasticsearch");
        Settings settings = Settings.builder().put(settingConfig).build();
        client = TransportClient.builder().settings(settings).build();
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 19300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    public static void closeClient() {
        if (client != null) client.close();
    }

    public static String getIndexName() {
        return INDEX_NAME;
    }

    public static String getTypeName() {
        return TYPE_NAME;
    }


}
