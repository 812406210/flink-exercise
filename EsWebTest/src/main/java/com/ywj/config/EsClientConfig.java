package com.ywj.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @program: datacenter
 * @description: es工具类
 * @author: yang
 * @create: 2020-10-17 09:25
 */
@Component
public class EsClientConfig {
    private static final Log LOG = LogFactory.getLog(EsClientConfig.class);
    // ElasticSearch的集群信息
    @Value("${EsClient.cluster.name}")
    private String clusterName;
    @Value("${EsClient.client.transport.nodeList}")
    private String nodeList;
    @Value("${EsClient.client.transport.port}")
    private String port;

    private volatile TransportClient client = null;

    /**
     * init ES client
     */
    @PostConstruct
    public TransportClient initEsClient() {
        if (this.client == null) {
            synchronized (EsClientConfig.class) {
                if (this.client == null) {
                    LOG.info("---------- Init ES Client " + this.clusterName + " -----------");
                    Settings settings = Settings.builder().put("cluster.name", this.clusterName).build();

                    try {
                        this.client = new PreBuiltTransportClient(settings);
                        String[] nodes = this.nodeList.split(",");
                        for(String node : nodes) {
                            client.addTransportAddress(
                                    new TransportAddress(InetAddress.getByName(node), Integer.parseInt(this.port)));
                                  //  new InetSocketTransportAddress(InetAddress.getByName(node), Integer.parseInt(this.port)));
                        }
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return this.client;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getNodeList() {
        return nodeList;
    }

    public void setNodeList(String nodeList) {
        this.nodeList = nodeList;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
