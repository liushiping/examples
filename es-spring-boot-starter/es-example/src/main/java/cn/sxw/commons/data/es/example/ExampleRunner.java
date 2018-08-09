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
