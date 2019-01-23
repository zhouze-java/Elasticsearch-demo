package com.demo.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author 周泽
 * @date Create in 10:25 2019/1/23
 * @Description 汽车零售案例背景 会涉及到三个数据,汽车信息,汽车销售记录,汽车4S店信息
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class CarShopTests {

    @Autowired
    private TransportClient client;

    /**
     * 需求: 调整宝马320这个汽车的售价，我们希望将售价设置为32万，用一个upsert语法，如果这个汽车的信息之前不存在，那么就insert，如果存在，那么就update
     */
    @Test
    public void upsert() throws IOException, ExecutionException, InterruptedException {
        // index操作
        IndexRequest indexRequest = new IndexRequest("car_shop", "cars", "1")
                .source(jsonBuilder()
                    .startObject()
                        .field("brand", "宝马")
                        .field("name","宝马320")
                        .field("price",320000)
                        .field("produce_date", "2018-01-01")
                    .endObject()
                );

        // update操作
        UpdateRequest updateRequest = new UpdateRequest("car_shop", "cars", "1")
                .doc(jsonBuilder()
                    .startObject()
                        .field("price", 310000)
                    .endObject()
                ).upsert(indexRequest);

        // 客户端执行
        client.update(updateRequest).get();
    }

    /**
     * 需求: 批量查询document
     */
    @Test
    public void mgetTest(){
        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add("car_shop", "cars", "1")
                .add("car_shop", "cars", "2")
                .get();

        // 输出
        for (MultiGetItemResponse itemResponse : multiGetResponse) {
            GetResponse responses = itemResponse.getResponse();

            if (responses.isExists()){
                log.info("response:{}", responses.getSourceAsString());
            }
        }
    }

    /**
     * bulk批量操作Api
     */
    @Test
    public void bulkTest() throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        // index操作,添加一条销售记录进去
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("car_shop", "sales", "3")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("brand", "奔驰")
                        .field("name", "奔驰C200")
                        .field("price", 350000)
                        .field("produce_date", "2017-01-05")
                        .field("sale_price", 340000)
                        .field("sale_date", "2017-02-03")
                    .endObject()
                );

        // update操作 更新一条id是1的数据
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("car_shop", "sales", "1")
                .setDoc(jsonBuilder()
                    .startObject()
                        .field("sale_price", 290000)
                    .endObject()
                );

        // 删除操作 删除id是2的数据
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("car_shop", "sales", "2");

        // 请求都添加到bulk中
        bulkRequestBuilder.add(indexRequestBuilder)
                .add(updateRequestBuilder)
                .add(deleteRequestBuilder);

        // 发送请求
        BulkResponse responses = bulkRequestBuilder.get();

    }

    /**
     * scroll Api 滚动查询
     */
    @Test
    public void scrollTests(){

        // scroll查询 时间为60s,查询是宝马的数据每次查询一条
        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("sales")
                .setScroll(new Scroll(new TimeValue(60000)))
                .setQuery(QueryBuilders.termQuery("brand.keyword", "宝马"))
                .setSize(1)
                .get();

        // 继续往下查询
        while (response.getHits().getHits().length > 0){
            for (SearchHit hit : response.getHits().getHits()) {
                // 拿到每条数据去处理
                log.info("hit:{}", hit.getSourceAsString());
            }

            // 继续下一次查询
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();

        }
    }

    /**
     * 搜索模板的调用
     */
    @Test
    public void searchTemplate(){
        // 请求参数
        Map<String,Object> map = new HashMap<>(3);
        map.put("from", 0);
        map.put("size", 1);
        map.put("brand", "宝马");

        SearchResponse response = new SearchTemplateRequestBuilder(client)
                .setScript("page_query_by_brand")
                .setScriptType(ScriptType.FILE)
                .setScriptParams(map)
                .setRequest(new SearchRequest("car_shop").types("sales"))
                .get()
                .getResponse();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    /**
     * 全文检索
     */
    @Test
    public void matchQuery(){
        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.matchQuery("brand", "宝马"))
                .get();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    @Test
    public void multiMatchQuery(){
        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.multiMatchQuery("宝马", "brand", "name"))
                .get();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    @Test
    public void termQuery(){
        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.termQuery("name.raw", "宝马320"))
                .get();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    @Test
    public void prefixQuery(){
        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.prefixQuery("name", "宝"))
                .get();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    @Test
    public void boolQuery(){
        // 组装查询条件
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("brand", "宝马"))
                .mustNot(QueryBuilders.termQuery("name.raw", "宝马318"))
                .should(QueryBuilders.rangeQuery("produce_date").gte("2017-01-01").lte("2017-01-31"))
                .filter(QueryBuilders.rangeQuery("price").gte(280000).lte(350000));

        // 然后调用搜索接口
        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(queryBuilder)
                .get();

        // 输出
        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    /**
     * 矩形范围查询
     */
    @Test
    public void getBoundingBoxQuery(){

        QueryBuilder queryBuilder = QueryBuilders.geoBoundingBoxQuery("pin.location").setCorners(40.73, -74.1, 40.01, -71.12);

        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("shops")
                .setQuery(queryBuilder)
                .get();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    /**
     * 多坐标点区域查询
     */
    @Test
    public void geoPolygonQuery(){

        // 多个坐标点
        List<GeoPoint> points = new ArrayList<>(3);
        points.add(new GeoPoint(40.73, -74.1));
        points.add(new GeoPoint(40.01, -71.12));
        points.add(new GeoPoint(50.56, -90.58));

        // 查询条件
        QueryBuilder queryBuilder = QueryBuilders.geoPolygonQuery("pin.location", points);

        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("shops")
                .setQuery(queryBuilder)
                .get();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }

    /**
     *
     */
    @Test
    public void geoDistanceQuery(){

        // 查询条件
        QueryBuilder queryBuilder = QueryBuilders.geoDistanceQuery("pin.location")
                .point(40, -70)
                .distance(200, DistanceUnit.KILOMETERS);

        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("shops")
                .setQuery(queryBuilder)
                .get();

        for (SearchHit hit : response.getHits().getHits()) {
            log.info("hit:{}", hit.getSourceAsString());
        }
    }
}
