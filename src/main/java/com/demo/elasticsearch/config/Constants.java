package com.demo.elasticsearch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author 周泽
 * @date Create in 11:12 2018/11/28
 * @Description 配置类
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix="com.demo.elasticsearch.config")
public class Constants {

    private String elasticsearchAddress;

    private int elasticsearchPort;

}
