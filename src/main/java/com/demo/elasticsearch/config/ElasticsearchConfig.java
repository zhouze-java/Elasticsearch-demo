package com.demo.elasticsearch.config;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * @author 周泽
 * @date Create in 14:21 2018/11/28
 * @Description es 配置类
 */
@Configuration
@Slf4j
public class ElasticsearchConfig {

    /**
     * ip地址
     */
    @Value("${elasticsearch.ip}")
    private String hostName;

    @Value("${elasticsearch.port}")
    private int port;

    @Value("${elasticsearch.pool}")
    private int poolSize;

    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    @Bean
    public TransportClient init(){
        TransportClient transportClient = null;
        try {
            // 配置
            Settings settings = Settings.builder()
                    .put("cluster.name", clusterName)
                    // 集群嗅探机制,找到es集群
                    .put("client.transport.sniff", true)
                    // 增加线程池个数
                    .put("thread_pool.search.size", poolSize)
                    .build();

            transportClient = new PreBuiltTransportClient(settings)
                    // 设置地址端口号
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));

        } catch (Exception e){
            log.error("elasticsearch TransportClient init error,{}", e);
        }

        return transportClient;
    }
}
