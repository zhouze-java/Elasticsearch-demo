package com.demo.elasticsearch.util;

import com.demo.elasticsearch.model.PageVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author 周泽
 * @date Create in 14:51 2018/11/28
 * @Description es操作工具类
 */
@Component
@Slf4j
public class ElasticsearchUtils {

    @Autowired
    private TransportClient transportClient;

    private static TransportClient client;

    @PostConstruct
    public void init(){
        client = this.transportClient;
    }

    /**
     * 判断索引是否存在
     * @param indexName 索引名称
     * @return true/false
     */
    public static boolean indexExist(String indexName){
        IndicesExistsResponse indicesExistsResponse = client.admin()
                .indices()
                .exists(new IndicesExistsRequest(indexName))
                .actionGet();

        if (indicesExistsResponse.isExists()){
            log.info("Index ['{}'] is exists", indexName);
        } else {
            log.info("Index ['{}'] is not exists", indexName);
        }

        return indicesExistsResponse.isExists();
    }

    /**
     * 创建索引
     * @param indexName 索引名称
     * @return isAcknowledged
     */
    public static boolean createIndex(String indexName){

        if (!indexExist(indexName)){
            log.info("Index is not exist");
        }

        CreateIndexResponse response = client.admin()
                .indices()
                .prepareCreate(indexName)
                .execute()
                .actionGet();

        return response.isAcknowledged();

    }

    /**
     * 删除索引
     * @param indexName 索引名称
     * @return isAcknowledged
     */
    public static boolean deleteIndex(String indexName){

        if (!indexExist(indexName)){
            log.info("Index is not exist");
        }

        DeleteIndexResponse response = client.admin()
                .indices()
                .prepareDelete(indexName)
                .execute()
                .actionGet();

        return response.isAcknowledged();
    }

    /**
     * 创建一个document,需要手动指定id
     * @param indexName 索引名称
     * @param typeName 类型名称
     * @param id id
     * @param xContentBuilder 数据(fields)
     * @return id
     */
    public static String createDocument(String indexName, String typeName, String id, XContentBuilder xContentBuilder){

        IndexResponse response = client
                .prepareIndex(indexName, typeName, id)
                .setSource(xContentBuilder)
                .get();

        log.info("add document response:{}", response.toString());

        return response.getId();
    }

    /**
     * 创建一个document,不需要手动指定id
     * @param indexName 索引名称
     * @param typeName 类型名称
     * @param xContentBuilder 数据(fields)
     * @return id
     */
    public static String createDocumentWithNoId(String indexName, String typeName, XContentBuilder xContentBuilder){

        IndexResponse response = client
                .prepareIndex(indexName, typeName)
                .setSource(xContentBuilder)
                .get();

        log.info("add document response:{}", response.toString());

        return response.getId();
    }

    /**
     * 更新document,partial update
     * @param indexName 索引名称
     * @param typeName 类型名称
     * @param id id
     * @param xContentBuilder 数据
     * @return id
     */
    public static String updateDocument(String indexName, String typeName, String id, XContentBuilder xContentBuilder){

        UpdateResponse updateResponse = client
                .prepareUpdate(indexName, typeName, id)
                .setDoc(xContentBuilder)
                .get();

        log.info("update response:{}", updateResponse.toString());

        return updateResponse.getId();
    }

    /**
     * 删除document
     * @param indexName 索引名称
     * @param typeName 类型名称
     * @param id id
     * @return id
     */
    public static String deleteDocument(String indexName, String typeName, String id){

        DeleteResponse response = client
                .prepareDelete(indexName, typeName, id)
                .get();

        log.info("delete response:{}", response.toString());

        return response.getId();
    }

    /**
     * 根据id获取document
     * @param indexName 索引名称
     * @param typeName 类型名称
     * @param id id
     * @return _source数据
     */
    public static String getDocumentById(String indexName, String typeName, String id){

        GetResponse response = client
                .prepareGet(indexName, typeName, id)
                .get();

        log.info("get response");

        return response.getSourceAsString();
    }

    /**
     * 只做查询,没有排序
     * @param indexes 索引
     * @param types 类型
     * @param matchMap 搜索条件
     * @param fields 要显示的fields,不传返回全部
     * @return 结果集
     */
    public static List<Map<String,Object>> searchDocument(String indexes, String types, Map<String,String> matchMap, String fields){
        return searchDocument(indexes, types, 0, 0, matchMap, false, null, fields, null, null, null);
    }


    /**
     * 查询/精准匹配,可以排序
     * @param indexes 索引
     * @param types 类型
     * @param matchMap 查询条件
     * @param fields 要显示的fields,不传返回全部
     * @param matchPhrase true 使用短语精准匹配
     * @param sortField 排序field
     * @param sortOrder 正序倒序(正序的话需要字段有正排索引)
     * @return 结果集
     */
    public static List<Map<String,Object>> searchDocument(String indexes, String types, Map<String,String> matchMap, String fields, boolean matchPhrase, String sortField, SortOrder sortOrder){
        return searchDocument(indexes, types, 0, 0, matchMap, matchPhrase, null, fields, sortField, sortOrder, null);
    }

    /**
     * 查询/精准匹配,可以排序,高亮,文档大小限制
     * @param indexes 索引
     * @param types 类型
     * @param matchMap 查询条件
     * @param fields 要显示的fields,不传返回全部
     * @param matchPhrase true 使用短语精准匹配
     * @param sortField  排序field
     * @param sortOrder 正序倒序(正序的话需要字段有正排索引)
     * @param highlightField 高亮字段
     * @param size 文档大小限制
     * @return 结果集
     */
    public static List<Map<String,Object>> searchDocument(String indexes, String types, Map<String,String> matchMap, String fields, boolean matchPhrase, String sortField,
                                                          SortOrder sortOrder, String highlightField, Integer size){
        return searchDocument(indexes, types, 0, 0, matchMap, matchPhrase, highlightField, fields, sortField, sortOrder, size);
    }

    /**
     * 搜索document
     * @param indexes 索引名
     * @param types 类型
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @param matchMap 查询条件(filed:value)
     * @param matchPhrase true 使用短语精准匹配
     * @param highlightField 高亮显示的field
     * @param fields 要显示的fields,不传返回全部
     * @param sortField 排序field
     * @param sortOrder 正序倒序(正序的话需要字段有正排索引)
     * @param size 文档大小限制
     * @return 结果集
     */
    public static List<Map<String, Object>> searchDocument(String indexes, String types, long startTime, long endTime, Map<String,String> matchMap, boolean matchPhrase,
                                                           String highlightField, String fields, String sortField, SortOrder sortOrder, Integer size){
        if (StringUtils.isEmpty(indexes)){
            return null;
        }

        // 构建查询的request body
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexes.split(","));

        // 拆分type
        if (StringUtils.isNotEmpty(types)){
            searchRequestBuilder.setTypes(types.split(","));
        }

        // 组合查询 bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // 组装查询条件
        boolQueryBuilder = boolQuery(boolQueryBuilder, startTime, endTime, matchMap, matchPhrase);

        // 设置高亮字段
        searchRequestBuilder = setHighlightField(searchRequestBuilder, highlightField);

        // 搜索条件加到request中
        searchRequestBuilder.setQuery(boolQueryBuilder);

        // 定制返回的fields
        if (StringUtils.isNotEmpty(fields)){
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        searchRequestBuilder.setFetchSource(true);

        // 设置排序
        if (StringUtils.isNotEmpty(sortField)){
            searchRequestBuilder.addSort(sortField, sortOrder);
        }

        // 设置文档大小限制
        if (size != null && size > 0){
            searchRequestBuilder.setSize(size);
        }

        // 把请求体打印出来
        log.info("查询请求体:{}", searchRequestBuilder);

        // 发送请求,执行查询
        SearchResponse response = searchRequestBuilder
                .execute()
                .actionGet();

        long totalHits = response.getHits().totalHits();
        long length = response.getHits().getHits().length;

        log.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (response.status().getStatus() == 200){
            return setSearchResponse(response, highlightField);
        }

        return null;
    }

    /**
     * 分页查询
     * @param indexes 索引
     * @param types 类型
     * @param pageNum 页码
     * @param pageSize 每页显示数量
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @param fields 要显示的字段
     * @param sortField 排序字段
     * @param sortOrder 正序倒序(正序需要排序的字段有正排索引)
     * @param matchPhrase true 精准匹配
     * @param highlightField 高亮子弹
     * @param matchMap 查询条件
     * @return PageVO
     */
    public static PageVO searchDocumentPage(String indexes, String types, int pageNum, int pageSize, long startTime, long endTime, String fields, String sortField,
                                            SortOrder sortOrder, boolean matchPhrase, String highlightField, Map<String,String> matchMap){
        if (StringUtils.isEmpty(indexes)){
            return null;
        }

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexes.split(","));

        if (StringUtils.isNotEmpty(types)){
            searchRequestBuilder.setTypes(types.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 设置需要显示的字段
        if (StringUtils.isNotEmpty(fields)){
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        // 设置排序字段
        if (StringUtils.isNotEmpty(sortField)){
            searchRequestBuilder.addSort(sortField, sortOrder);
        }

        // 组合查询 bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // 组装查询条件
        boolQueryBuilder = boolQuery(boolQueryBuilder, startTime, endTime, matchMap, matchPhrase);

        // 设置高亮字段
        searchRequestBuilder = setHighlightField(searchRequestBuilder, highlightField);

        // 搜索条件加到request中
        searchRequestBuilder.setQuery(boolQueryBuilder);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        // 设置分页
        searchRequestBuilder.setFrom(pageNum).setSize(pageSize);

        // 设置按照匹配度排序
        searchRequestBuilder.setExplain(true);

        // 打印请求体
        log.info("请求体:{}", searchRequestBuilder);

        // 发送请求,执行查询
        SearchResponse response = searchRequestBuilder
                .execute()
                .actionGet();

        long totalHits = response.getHits().totalHits();
        long length = response.getHits().getHits().length;

        log.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (response.status().getStatus() == 200){
            // 解析查询对象
            List<Map<String,Object>> rList = setSearchResponse(response, highlightField);

            return new PageVO(pageNum, pageSize, (int) totalHits, rList);
        }

        return null;
    }

    /**
     * 高亮结果集 特殊处理
     * @param searchResponse 查询返回结果
     * @param highlightField 高亮字段
     * @return 结果
     */
    public static List<Map<String,Object>> setSearchResponse(SearchResponse searchResponse, String highlightField){
        List<Map<String,Object>> sourceList = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();

        // 循环查询结果
        for (SearchHit searchHitFields : searchResponse.getHits().getHits()) {
            // 把id放到_source里面去
            searchHitFields.getSource().put("id", searchHitFields.getId());

            // 有高亮字段的话做处理
            if (StringUtils.isNotEmpty(highlightField)){
                log.info("遍历高亮结果集,覆盖正常结果集...{}", searchHitFields.getSource());

                Text[] texts = searchHitFields.getHighlightFields().get(highlightField).getFragments();

                if (texts != null){
                    for (Text text : texts) {
                        stringBuilder.append(text.toString());
                    }
                    // 遍历高亮结果集,覆盖正常结果集
                    searchHitFields.getSource().put(highlightField, stringBuilder.toString());
                }
            }

            sourceList.add(searchHitFields.getSource());
        }

        return sourceList;
    }

    /**
     * 封装
     * @param boolQueryBuilder boolQueryBuilder
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @param matchMap 查询条件
     * @param matchPhrase true 使用精准匹配
     * @return boolQueryBuilder
     */
    public static BoolQueryBuilder boolQuery(BoolQueryBuilder boolQueryBuilder, long startTime, long endTime, Map<String, String> matchMap, boolean matchPhrase){
        // TODO 不清楚是做什么
        if (startTime > 0 && endTime > 0){
            boolQueryBuilder.must(QueryBuilders.rangeQuery("processTime")
                    .format("epoch_millis")
                    .from(startTime)
                    .to(endTime)
                    .includeLower(true)
                    .includeUpper(true)
            );
        }

        // 搜索条件
        if (!matchMap.isEmpty()){
            for (Map.Entry<String,String> entry : matchMap.entrySet()) {
                if (StringUtils.isNoneBlank(entry.getKey(),entry.getValue())){
                    if (matchPhrase == Boolean.TRUE){
                        // 精准匹配
                        boolQueryBuilder.must(QueryBuilders.matchPhraseQuery(entry.getKey(), entry.getValue()));
                    } else {
                        boolQueryBuilder.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }

        return boolQueryBuilder;
    }

    /**
     * 封装设置高亮字段
     * @param searchRequestBuilder searchRequestBuilder
     * @param highlightField 高亮字段
     * @return searchRequestBuilder
     */
    public static SearchRequestBuilder setHighlightField(SearchRequestBuilder searchRequestBuilder, String highlightField){
        // 高亮字段
        if (StringUtils.isNotEmpty(highlightField)){
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            // 设置前缀
//            highlightBuilder.preTags("<span style='color:red'>");
            // 设置后缀
//            highlightBuilder.postTags("</span>");
            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        return searchRequestBuilder;
    }
}
