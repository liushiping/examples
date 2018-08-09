package cn.sxw.commons.data.es.starter;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by William on 2018/8/7.
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(ElasticSearchProperties.class)
public class ElasticSearchAutoConfiguration implements DisposableBean{

    private TransportClient transportClient;
    @Resource
    private ElasticSearchProperties properties;

    @Bean
    @ConditionalOnMissingBean(TransportClient.class)
    public TransportClient transportClient() {
        log.info("=======" + properties.getClusterName());
        log.info("=======" + properties.getClusterNodes());
        log.info("=======" + properties.getUserName());
        log.info("=======" + properties.getPassword());
        log.info("开始建立es连接");
        transportClient = new PreBuiltXPackTransportClient(settings());
        TransportAddress[] transportAddresses= Arrays.stream(properties.getClusterNodes().split(",")).map (t->{
            String[] addressPortPairs = t.split(":");
            String address = addressPortPairs[0];
            Integer port = Integer.valueOf(addressPortPairs[1]);
            try {
                return new InetSocketTransportAddress(InetAddress.getByName(address), port);
            } catch (UnknownHostException e) {
                log.error("", e);
                throw new RuntimeException ("连接ElasticSearch失败",e);
            }
        }).collect (Collectors.toList ()).toArray (new TransportAddress[0]);
        transportClient.addTransportAddresses(transportAddresses);
        return transportClient;
    }

    private Settings settings() {
        return Settings.builder()
                .put("cluster.name", properties.getClusterName())
                .put("xpack.security.user", properties.getUserName() +
                        ":" + properties.getPassword())
                .build();
    }

    @Override
    public void destroy() throws Exception {
        log.info("开始销毁Es的连接");
        if (transportClient != null) {
            transportClient.close();
        }
    }
}
