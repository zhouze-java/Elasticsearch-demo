package com.demo.elasticsearch;

import com.demo.elasticsearch.model.PageVO;
import com.demo.elasticsearch.util.ElasticsearchUtils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class AppTests {

    @Test
    public void contextLoads() {
    }

    @Test
    public void createIndex(){
        ElasticsearchUtils.createIndex("my_index");
    }

    @Test
    public void deleteIndex(){
        ElasticsearchUtils.deleteIndex("my_index");
    }

    @Test
    public void createDocumentWithId(){
        try {
            // 创建数据
            XContentBuilder xContentBuilder = jsonBuilder()
                    .startObject()
                    .field("name", "zhangsan")
                    .field("age", 18)
                    .field("date", "2018-01-01")
                    .endObject();

            ElasticsearchUtils.createDocument("my_index", "my_type", "1", xContentBuilder);
        } catch (IOException e) {
            log.info("xContentBuilder 错误:{}", e);
        }
    }

    @Test
    public void createDocumentWithNoId(){
        try {
            // 创建数据
            XContentBuilder xContentBuilder = jsonBuilder()
                    .startObject()
                    .field("name", "lisi")
                    .field("age", 30)
                    .field("date", "2018-02-02")
                    .endObject();

            ElasticsearchUtils.createDocumentWithNoId("my_index", "my_type", xContentBuilder);
        } catch (IOException e){
            log.info("xContentBuilder 错误:{}", e);
        }
    }

    @Test
    public void updateDocument(){
        try {
            // 创建数据
            XContentBuilder xContentBuilder = jsonBuilder()
                    .startObject()
                    .field("date", "2013-02-02")
                    .endObject();

            ElasticsearchUtils.updateDocument("my_index", "my_type", "1", xContentBuilder);

        } catch (IOException e){
            log.info("xContentBuilder 错误:{}", e);
        }
    }

    @Test
    public void deleteDocument(){
        ElasticsearchUtils.deleteDocument("my_index", "my_type", "1");
    }

    @Test
    public void getDocumentById(){
        String source = ElasticsearchUtils.getDocumentById("my_index", "my_type", "AWdZSQ2xtgd1GzpTfkJN");

        log.info("_source: {}", source);
    }

    @Test
    public void searchDocument(){
        Map<String,String> fieldMap = new HashMap<>();
        fieldMap.put("title", "kill");
        List<Map<String,Object>> lists = ElasticsearchUtils.searchDocument("movies", "movie", 0, 0, fieldMap, false, "title", null, "year", SortOrder.DESC, null);

        for (Map<String, Object> list : lists) {
            for (Map.Entry<String, Object> entry : list.entrySet()) {
                log.info("field:{}, value:{}", entry.getKey(), entry.getValue());
            }
        }
    }

    @Test
    public void searchDocumentPage(){
        Map<String,String> fieldMap = new HashMap<>();
        fieldMap.put("title", "kill");

        PageVO pageVO = ElasticsearchUtils.searchDocumentPage("movies", "movie", 1, 2, 0, 0, null, "year", SortOrder.DESC, false, null, fieldMap);

        log.info("pageNum:{}", pageVO.getPageNum());
        log.info("pageSize:{}", pageVO.getPageSize());
        log.info("BeginPageIndex:{}", pageVO.getBeginPageIndex());
        log.info("endPageIndex:{}", pageVO.getEndPageIndex());
        log.info("pageCount:{}", pageVO.getPageCount());
        log.info("total:{}", pageVO.getTotal());

        for (Map<String, Object> list : pageVO.getRList()) {
            for (Map.Entry<String, Object> entry : list.entrySet()) {
                log.info("field:{}, value:{}", entry.getKey(), entry.getValue());
            }
        }
    }


    @Test
    public void test() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("car_shop", "cars", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("price", 310000)
                        .endObject())
                .upsert();
    }
}
