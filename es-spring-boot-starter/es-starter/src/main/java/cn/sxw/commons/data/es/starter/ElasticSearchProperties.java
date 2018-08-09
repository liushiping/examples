package cn.sxw.commons.data.es.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * Created by William on 2018/8/7.
 */
@Data
@ConfigurationProperties(prefix = "sxw.elasticsearch")
public class ElasticSearchProperties {

    private String clusterName = "elasticsearch";

    private String clusterNodes = "127.0.0.1:9300";

    private String userName = "elastic";

    private String password = "changeme";

}
