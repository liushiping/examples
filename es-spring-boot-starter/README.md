# 一.Spring Boot Starter简介
Starter是Spring Boot中的一个非常重要的概念，Starter相当于模块，它能将模块所需的依赖整合起来并对模块内的Bean根据环境（ 条件）进行自动配置。使用者只需要依赖相应功能的Starter，无需做过多的配置和依赖，Spring Boot就能自动扫描并加载相应的模块。

例如在Maven的依赖中加入spring-boot-starter-web就能使项目支持Spring MVC，并且Spring Boot还为我们做了很多默认配置，无需再依赖spring-web、spring-webmvc等相关包及做相关配置就能够立即使用起来。

# 二.Starter的开发步骤
编写Starter非常简单，与编写一个普通的Spring Boot应用没有太大区别，总结如下：

    1.新建Maven项目，在项目的POM文件中定义使用的依赖；
    2.新建配置类，写好配置项和默认的配置值，指明配置项前缀；
    3.新建自动装配类，使用@Configuration和@Bean来进行自动装配；
    4.新建spring.factories文件，指定Starter的自动装配类；

# 三.Starter的开发示例
下面，我就以创建一个自动配置并连接ElasticSearch的Starter来讲一下各个步骤及细节。
1.新建Maven项目，在项目的POM文件中定义使用的依赖。
```
    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <parent>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-parent</artifactId>
            <version>2.0.4.RELEASE</version>
        </parent>
        <modelVersion>4.0.0</modelVersion>
    
        <artifactId>es-starter</artifactId>
        <version>1.0.0.SNAPSHORT</version>
    
        <dependencies>
            <dependency>
                <groupId>org.projectlombok</groupId>
                <artifactId>lombok</artifactId>
                <version>1.16.18</version>
            </dependency>
    
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-starter</artifactId>
                <version>2.0.4.RELEASE</version>
            </dependency>
    
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-configuration-processor</artifactId>
                <optional>true</optional>
            </dependency>
    
            <dependency>
                <groupId>org.elasticsearch.client</groupId>
                <artifactId>x-pack-transport</artifactId>
                <version>5.6.4</version>
            </dependency>
        </dependencies>
    </project>
```
> 由于本starter主要是与ElasticSearch建立连接，获得TransportClient对象，所以需要依赖`x-pack-transport`包。

2.新建配置类，写好配置项和默认的配置值，指明配置项前缀。
```
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
```
> 指定配置项前缀为`sxw.elasticsearch`，各配置项均有默认值，默认值可以通过模块使用者的配置文件进行覆盖。

3.新建自动装配类，使用`@Configuration`和`@Bean`来进行自动装配。
```
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
        log.debug("=======" + properties.getClusterName());
        log.debug("=======" + properties.getClusterNodes());
        log.debug("=======" + properties.getUserName());
        log.debug("=======" + properties.getPassword());
        log.info("开始建立es连接");
        transportClient = new PreBuiltXPackTransportClient(settings());
        TransportAddress[] transportAddresses= Arrays.stream(properties.getClusterNodes().split(",")).map (t->{
            String[] addressPortPairs = t.split(":");
            String address = addressPortPairs[0];
            Integer port = Integer.valueOf(addressPortPairs[1]);
            try {
                return new InetSocketTransportAddress(InetAddress.getByName(address), port);
            } catch (UnknownHostException e) {
                log.error("连接ElasticSearch失败", e);
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
```
> 本类主要对TransportClient类进行自动配置;
`@ConditionalOnMissingBean` 当Spring容器中没有TransportClient类的对象时，调用`transportClient()`创建对象;
关于更多Bean的条件装配用法请自行查阅Spring Boot相关文档;

4.新建spring.factories文件，指定Starter的自动装配类。
```
org.springframework.boot.autoconfigure.EnableAutoConfiguration=\
  cn.sxw.commons.data.es.starter.ElasticSearchAutoConfiguration
```
> spring.factories文件位于resources/META-INF目录下，需要手动创建;
`org.springframework.boot.autoconfigure.EnableAutoConfiguration`后面的类名说明了自动装配类，如果有多个 ，则用逗号分开;
使用者应用（SpringBoot）在启动的时候，会通过`org.springframework.core.io.support.SpringFactoriesLoader`读取classpath下每个Starter的spring.factories文件，加载自动装配类进行Bean的自动装配；

至此，整个Starter开发完毕，Deploy到中央仓库或Install到本地仓库后即可使用。
# 四.Starter的使用
1.创建Maven项目，依赖刚才发布的es-starter包。
```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>spring-boot-parent</artifactId>
        <groupId>org.springframework.boot</groupId>
        <version>2.0.4.RELEASE</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>es-example</artifactId>
    <version>1.0.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>es-starter</artifactId>
            <version>1.0.0.SNAPSHORT</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```
> 只需依赖刚才开发的es-starter即可

2.编写应用程序启动类。
```
package cn.sxw.commons.data.es.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Created by William on 2018/8/7.
 */
@SpringBootApplication
@ComponentScan("cn.sxw.commons.data.es.example")
public class ExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }
}
```
3.编写查询ElasticSearch的使用类
```
package cn.sxw.commons.data.es.example;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by William on 2018/8/7.
 */
@Slf4j
@Component
public class ExampleRunner implements ApplicationRunner {

    private static final String INDEX_NAME = "tb_question";

    @Autowired
    private TransportClient transportClient;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        SearchResponse response = transportClient.prepareSearch(INDEX_NAME)
                .setTypes(INDEX_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0).setSize(5).execute().actionGet();
        SearchHits hits = response.getHits();
        log.info(String.format("=======总共找到%d条记录", hits.getTotalHits()));
        log.info("=======第一页数据：");
        for (SearchHit searchHit : hits) {
            Map<String, Object> source = searchHit.getSource();
            String question = source.get("question").toString();
            log.info(question);
        }
    }
}
```
> 通过实现ApplicationRunner或CommandLineRunner接口，可以实现应用程序启动完成后自动运行run方法，达到测试es-starter模块目的。
索引名称tb_question是公司测试环境ElasticSearch中的索引，已存在数据。

4.应用程序配置
```
sxw:
  elasticsearch:
    cluster-name: docker-cluster
    cluster-nodes: 192.168.2.180:9300,192.168.2.181:9300
    user-name: elastic
    password: changeme
```
> 在application.yml文件中配置es-starter需要的配置信息，这里连接公司测试环境中的ElasticSearch。
这里配置的值可以覆盖es-starter中默认值，也就是之前ElasticSearchProperties文件中的默认值。

5.运行程序测试
```
/Library/Java/JavaVirtualMachines/jdk1.8.0_25.jdk/Contents/Home/bin/java "-javaagent:/Applications/开发/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=52434:/Applications/开发/IntelliJ IDEA.app/Contents/bin" -Dfile.encoding=UTF-8 -classpath cn.sxw.commons.data.es.example.ExampleApplication
objc[2017]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.8.0_25.jdk/Contents/Home/bin/java (0x1022a24c0) and /Library/Java/JavaVirtualMachines/jdk1.8.0_25.jdk/Contents/Home/jre/lib/libinstrument.dylib (0x1023254e0). One of the two will be used. Which one is undefined.

  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::        (v2.0.4.RELEASE)

2018-08-08 16:26:43.161  INFO 2017 --- [           main] c.s.c.d.es.example.ExampleApplication    : Starting ExampleApplication on William.local with PID 2017 (/Users/William/Git/sxw-java/es-spring-boot-starter/es-example/target/classes started by William in /Users/William/Git/sxw-java/es-spring-boot-starter)
2018-08-08 16:26:43.167  INFO 2017 --- [           main] c.s.c.d.es.example.ExampleApplication    : No active profile set, falling back to default profiles: default
2018-08-08 16:26:43.365  INFO 2017 --- [           main] s.c.a.AnnotationConfigApplicationContext : Refreshing org.springframework.context.annotation.AnnotationConfigApplicationContext@635eaaf1: startup date [Wed Aug 08 16:26:43 CST 2018]; root of context hierarchy
2018-08-08 16:26:45.078  INFO 2017 --- [           main] s.c.d.e.s.ElasticSearchAutoConfiguration : =======docker-cluster
2018-08-08 16:26:45.079  INFO 2017 --- [           main] s.c.d.e.s.ElasticSearchAutoConfiguration : =======192.168.2.180:9300,192.168.2.181:9300
2018-08-08 16:26:45.081  INFO 2017 --- [           main] s.c.d.e.s.ElasticSearchAutoConfiguration : =======elastic
2018-08-08 16:26:45.081  INFO 2017 --- [           main] s.c.d.e.s.ElasticSearchAutoConfiguration : =======changeme
2018-08-08 16:26:45.082  INFO 2017 --- [           main] s.c.d.e.s.ElasticSearchAutoConfiguration : 开始建立es连接
2018-08-08 16:26:46.200  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : no modules loaded
2018-08-08 16:26:46.201  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : loaded plugin [org.elasticsearch.index.reindex.ReindexPlugin]
2018-08-08 16:26:46.202  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : loaded plugin [org.elasticsearch.join.ParentJoinPlugin]
2018-08-08 16:26:46.202  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : loaded plugin [org.elasticsearch.percolator.PercolatorPlugin]
2018-08-08 16:26:46.202  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : loaded plugin [org.elasticsearch.script.mustache.MustachePlugin]
2018-08-08 16:26:46.202  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : loaded plugin [org.elasticsearch.transport.Netty3Plugin]
2018-08-08 16:26:46.202  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : loaded plugin [org.elasticsearch.transport.Netty4Plugin]
2018-08-08 16:26:46.202  INFO 2017 --- [           main] o.elasticsearch.plugins.PluginsService   : loaded plugin [org.elasticsearch.xpack.XPackPlugin]
2018-08-08 16:26:49.137  INFO 2017 --- [           main] o.s.j.e.a.AnnotationMBeanExporter        : Registering beans for JMX exposure on startup
2018-08-08 16:26:49.157  INFO 2017 --- [           main] c.s.c.d.es.example.ExampleApplication    : Started ExampleApplication in 6.6 seconds (JVM running for 7.915)
2018-08-08 16:26:49.215  INFO 2017 --- [           main] c.s.c.data.es.example.ExampleRunner      : =======总共找到907条记录
2018-08-08 16:26:49.215  INFO 2017 --- [           main] c.s.c.data.es.example.ExampleRunner      : =======第一页数据：
2018-08-08 16:26:49.230  INFO 2017 --- [           main] c.s.c.data.es.example.ExampleRunner      : <p>下列诗句朗读节奏有错误的一项是(  )</p>
2018-08-08 16:26:49.230  INFO 2017 --- [           main] c.s.c.data.es.example.ExampleRunner      : <p><span style=";font-family:宋体;color:rgb(0,0,0);font-size:14px"><span style="font-family:宋体">《卧薪尝胆》这个故事出自于（</span> A &nbsp;&nbsp;<span style="font-family:宋体">）</span></span></p><p><span style=";font-family:宋体;color:rgb(0,0,0);font-size:14px">A<span style="font-family:宋体">、司马迁《史记》 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span><span style="font-family:Times New Roman">B</span><span style="font-family:宋体">、司马光 《资治通鉴》</span></span></p><p><span style=";font-family:宋体;color:rgb(0,0,0);font-size:14px">C<span style="font-family:宋体">、孔子 &nbsp;&nbsp;《论语》 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span><span style="font-family:Times New Roman">D</span><span style="font-family:宋体">、司马迁《春秋》</span></span></p><p><br/></p>
2018-08-08 16:26:49.230  INFO 2017 --- [           main] c.s.c.data.es.example.ExampleRunner      : <p style="margin-bottom:7px;margin-bottom:auto;vertical-align:middle"><span style=";font-family:&#39;Cambria Math&#39;;font-size:14pxfont-family:宋体,新宋体">填空题</span></p><p style="margin-bottom:7px;margin-bottom:auto;vertical-align:middle"><span style=";font-family:&#39;Cambria Math&#39;;font-size:14pxfont-family:宋体,新宋体">该模式给当地带来的主要影响是 </span><span style=";font-family:&#39;Cambria Math&#39;;font-size:14px"><br/></span><br/></p><p><br/></p>
2018-08-08 16:26:49.231  INFO 2017 --- [           main] c.s.c.data.es.example.ExampleRunner      : <p>下列词语没有错别字的一项是(  )</p>
2018-08-08 16:26:49.231  INFO 2017 --- [           main] c.s.c.data.es.example.ExampleRunner      : <p>语文第16题</p>
2018-08-08 16:26:49.232  INFO 2017 --- [       Thread-2] s.c.a.AnnotationConfigApplicationContext : Closing org.springframework.context.annotation.AnnotationConfigApplicationContext@635eaaf1: startup date [Wed Aug 08 16:26:43 CST 2018]; root of context hierarchy
2018-08-08 16:26:49.234  INFO 2017 --- [       Thread-2] o.s.j.e.a.AnnotationMBeanExporter        : Unregistering JMX-exposed beans on shutdown
2018-08-08 16:26:49.328  INFO 2017 --- [       Thread-2] s.c.d.e.s.ElasticSearchAutoConfiguration : 开始销毁Es的连接

Process finished with exit code 0
```
> 运行程序，观察控制台输出，es-starter成功与ElasticSearch建立连接，且应用程序启动完后ExampleRunner的run方法查询出5条数据。

**源代码参考提供：**
[从零开始开发一个Spring Boot Starter](https://github.com/liushiping/examples/tree/master/es-spring-boot-starter)
