package com.es.demo.controller;

import com.es.demo.vo.InfoVO;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class EsApiController {
    @RequestMapping(value = "/es/test", method = RequestMethod.GET)
    public String esTest () {
        System.out.println("es test");
        return "es test";
    }

    // 문서생성
    @RequestMapping(value = "/es/doc/create", method = RequestMethod.GET)
    public Object esDocCreate () {
        String returnStr = "";
        try (RestHighLevelClient client = createConnection()) {

            String indexName = "customer";  // database개념
            String typeName  = "info";      // table 개념
            String docId     = "2";         // id개념 미지정시 ES에서 알아서 UUID로 생성해줌.. RDB시퀀스로도 많이 가져와서 대입.

            IndexRequest request = new IndexRequest(indexName,typeName,docId);

            request.source(
                    XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name","junghun")
                            .field("address","서울")
                            .field("phone","010-8664-5860")
                            .endObject()
            );

            IndexResponse response = client.index(request, RequestOptions.DEFAULT);

            System.out.println("response : "+response);

        } catch (ElasticsearchException ese) {
            ese.printStackTrace();
            if(ese.status().equals(RestStatus.CONFLICT)) {
                returnStr = "동일한 DOC_ID 문서가 존재";
            }
        } catch (Exception e) {
            e.printStackTrace();
            returnStr = "문서 생성에 실패";
        } finally {
        }
        return returnStr;
    }

    // ID값을 이용한 문서조회
    @RequestMapping(value = "/es/doc/searchById/{id}", method = RequestMethod.GET)
    public Object esDocSearchById (@PathVariable("id") String id) {
        String indexName = "customer";
        String typeName  = "info";
        String docId     = id;

        GetRequest getRequest = new GetRequest(indexName, typeName, docId);

        GetResponse getResponse = null;
        try (RestHighLevelClient client = createConnection()) {

            getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            e.printStackTrace();
            return "문서조회 중 오류발생.";
        }

        Gson gson = new Gson();
        Map<String, Object> map = null;
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            map = getResponse.getSourceAsMap();
        } else {
            return "해당 문서 미존재.";
        }

        return gson.toJson(map);
    }

    // 문서존재여부
    @RequestMapping(value = "/es/doc/existById/{id}", method = RequestMethod.GET)
    public Object esDocExistById (@PathVariable("id") String id) {
        String returnStr = "";

        String indexName = "customer";
        String typeName  = "info";
        String docId     = id;

        GetRequest request = new GetRequest(indexName, typeName, docId);

        boolean exist = false;
        try (RestHighLevelClient client = createConnection()) {

            exist = client.exists(request, RequestOptions.DEFAULT);

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (exist) {
            returnStr = "문서 존재함";
        } else {
            returnStr = "문서 미존재";
        }

        return returnStr;
    }

    // 문서삭제
    @RequestMapping(value = "/es/doc/deleteById/{id}", method = RequestMethod.GET)
    public Object esDocDeleteById (@PathVariable("id") String id) {
        String indexName = "customer";
        String typeName  = "info";
        String docId     = id;

        DeleteRequest request = new DeleteRequest(indexName, typeName, docId);
        DeleteResponse response = null;
        try (RestHighLevelClient client = createConnection()) {

            response = client.delete(request, RequestOptions.DEFAULT);

        }catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("response : "+response);

        return response;
    }

    // 문서수정
    @RequestMapping(value = "/es/doc/updateById/{id}", method = RequestMethod.GET)
    public Object esDocUpdateById (@PathVariable("id") String id) {
        String indexName = "customer";
        String typeName  = "info";
        String docId     = id;

        UpdateRequest request = null;
        UpdateResponse response = null;

        try (RestHighLevelClient client = createConnection()) {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("address","부산 ( 수정 )")
                    .endObject();

            request = new UpdateRequest(indexName, typeName, docId).doc(builder);

            response = client.update(request, RequestOptions.DEFAULT);

        } catch (ElasticsearchException e) {
            e.printStackTrace();
            if(e.status().equals(RestStatus.NOT_FOUND)) {
                return "수정할 문서가 존재하지 않습니다.";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("response : "+response);

        return response;
    }

    // 문서 upsert : 문서 존재시 update 미존재시 insert
    @RequestMapping(value = "/es/doc/upsertById/{id}", method = RequestMethod.GET)
    public Object esDocUpsertById (@PathVariable("id") String id) {
        String indexName = "customer";
        String typeName  = "info";
        String docId     = id;

        UpdateResponse response = null;

        try (RestHighLevelClient client = createConnection()) {
            IndexRequest index = new IndexRequest(indexName,typeName,docId);
            index.source(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("name","ann")
                    .field("address","전주")
                    .field("phone","010-8664-0000")
                    .endObject()
            );
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("phone","010-0000-0000")
                    .endObject();

            UpdateRequest request = new UpdateRequest(indexName, typeName, docId).doc(builder).upsert(index);

            response = client.update(request, RequestOptions.DEFAULT);

        }catch (ElasticsearchException e) {
            e.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("response : "+response);

        return response.getResult();
    }

    // 여러건의 문서 Create,Update,Delete
    @RequestMapping(value = "/es/doc/bulk", method = RequestMethod.GET)
    public Object esDocBulk () {
        String indexName = "customer";
        String typeName  = "info";

        BulkRequest request = new BulkRequest();
        BulkResponse response = null;

        try (RestHighLevelClient client = createConnection()) {

            request.add(new IndexRequest(indexName, typeName,"4")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name","vic")
                            .field("address","익산")
                            .field("phone","010-8664-0001")
                            .endObject()
                    )
            );
            request.add(new IndexRequest(indexName,typeName,"5")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name","nara")
                            .field("address","군산")
                            .field("phone","010-8664-0002")
                            .endObject()
                    )
            );
            request.add(new UpdateRequest(indexName,typeName,"2")
                    .doc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("address","부산 수정구 (수정)")
                            .endObject()
                    )
            );

            request.add(new DeleteRequest(indexName,typeName,"3"));

            response = client.bulk(request, RequestOptions.DEFAULT);

        }catch (Exception e) {
            /*
             * 예외처리
             */
            e.printStackTrace();
        }

        List<DocWriteResponse> results = new ArrayList<>();

        for(BulkItemResponse bulkItemResponse : response) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if(bulkItemResponse.getOpType().equals(DocWriteRequest.OpType.INDEX)) {
                IndexResponse indexResponse = (IndexResponse)itemResponse;
                results.add(indexResponse);
            } else if (bulkItemResponse.getOpType().equals(DocWriteRequest.OpType.UPDATE)) {
                UpdateResponse updateResponse = (UpdateResponse)itemResponse;
                results.add(updateResponse);
            } else if (bulkItemResponse.getOpType().equals(DocWriteRequest.OpType.DELETE)) {
                DeleteResponse deleteResponse = (DeleteResponse)itemResponse;
                results.add(deleteResponse);
            }
        }

        List<Object> resultList = results.stream().map(i->i.getResult()).collect(Collectors.toList());

        return Arrays.toString(resultList.toArray());
    }

    // 전체 서치
    @RequestMapping(value = "/es/doc/search/matchAll", method = RequestMethod.GET)
    public String esDocSearchMatchAll () {
        String indexName = "customer";
        String typeName  = "info";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //searchSourceBuilder.sort(new FieldSortBuilder("name").order(SortOrder.DESC)); 메모리 로드 이슈가 있음 (필드 설정시 fielddata = true 해줘야?? 아니면 keyword설정?)

        SearchRequest request = new SearchRequest(indexName);
        request.types(typeName);
        request.source(searchSourceBuilder);

        SearchResponse response = null;
        SearchHits searchHits = null;
        List<InfoVO> results = new ArrayList<>();

        try (RestHighLevelClient client = createConnection()) {
            response = client.search(request, RequestOptions.DEFAULT);
            searchHits = response.getHits();
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceMap = hit.getSourceAsMap();
                InfoVO infoVO = new InfoVO();
                infoVO.setName(sourceMap.get("name")+"");
                infoVO.setAddress(sourceMap.get("address")+"");
                infoVO.setPhone(sourceMap.get("phone")+"");
                results.add(infoVO);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }

        Gson gson = new Gson();

        return gson.toJson(results);
    }

    // 매칭되는 문자 서치
    @RequestMapping(value = "/es/doc/search/match/{matchStr}", method = RequestMethod.GET)
    public String esDocSearchMatch (@PathVariable("matchStr") String matchStr) {
        String indexName = "customer";
        String typeName  = "info";

        String fieldName = "phone";
        String searchStr = matchStr;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(fieldName, searchStr));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);

        SearchRequest request = new SearchRequest(indexName);
        request.types(typeName);
        request.source(searchSourceBuilder);

        SearchResponse response = null;
        SearchHits searchHits = null;
        List<InfoVO> resultMap = new ArrayList<>();

        try(RestHighLevelClient client = createConnection();){

            response = client.search(request, RequestOptions.DEFAULT);
            searchHits = response.getHits();
            for( SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                InfoVO infoVO = new InfoVO();
                infoVO.setName(sourceAsMap.get("name")+"");
                infoVO.setAddress(sourceAsMap.get("address")+"");
                infoVO.setPhone(sourceAsMap.get("phone")+"");
                resultMap.add(infoVO);
            }

        }catch (Exception e) {
            /*
             * 예외처리
             */
            e.printStackTrace();
        }

        Gson gson = new Gson();
        return gson.toJson(resultMap);
    }

    // 가중치 조절 매칭 서치 (많이 검색되는 단어와 적게 검색되는 단어 중 어떤 단어가 더 중요한지를 판단해서 검색 스코어링을 변경하는 알고리즘을 가지고 있는 쿼리)
    @RequestMapping(value = "/es/doc/search/commonTerm/{matchStr}", method = RequestMethod.GET)
    public String esDocSearchCommonTerm (@PathVariable("matchStr") String matchStr) {
        String indexName = "customer";
        String typeName  = "info";

        String fieldName = "phone";
        String searchStr = matchStr;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.commonTermsQuery(fieldName, searchStr));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //searchSourceBuilder.sort(new FieldSortBuilder("name").order(SortOrder.DESC));

        SearchRequest request = new SearchRequest(indexName);
        request.types(typeName);
        request.source(searchSourceBuilder);


        SearchResponse response = null;
        SearchHits searchHits = null;
        List<InfoVO> resultMap = new ArrayList<>();

        try(RestHighLevelClient client = createConnection();){
            response = client.search(request, RequestOptions.DEFAULT);
            searchHits = response.getHits();
            for( SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                InfoVO infoVO = new InfoVO();
                infoVO.setName(sourceAsMap.get("name")+"");
                infoVO.setAddress(sourceAsMap.get("address")+"");
                infoVO.setPhone(sourceAsMap.get("phone")+"");
                resultMap.add(infoVO);
            }
        }catch (Exception e) {
            /*
             * 예외처리
             */
            e.printStackTrace();
        }

        Gson gson = new Gson();
        return gson.toJson(resultMap);
    }

    // 쿼리문 자체에 AND, OR 절을 사용하고 싶을때 사용하는 쿼리
    @RequestMapping(value = "/es/doc/search/queryString/{matchStr}", method = RequestMethod.GET)
    public String esDocSearchQueryString (@PathVariable("matchStr") String matchStr) {
        String indexName = "customer";
        String typeName = "info";

        String fieldName = "phone";
        String searchStr = matchStr; //+010 -8664, +010 +8664 ...

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryStringQuery(searchStr).field(fieldName));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        // searchSourceBuilder.sort(new FieldSortBuilder("name").order(SortOrder.DESC));

        SearchRequest request = new SearchRequest(indexName);
        request.types(typeName);
        request.source(searchSourceBuilder);

        SearchResponse response = null;
        SearchHits searchHits = null;
        List<InfoVO> resultMap = new ArrayList<>();

        try(RestHighLevelClient client = createConnection();){
            response = client.search(request, RequestOptions.DEFAULT);
            searchHits = response.getHits();
            for( SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                InfoVO infoVO = new InfoVO();
                infoVO.setName(sourceAsMap.get("name")+"");
                infoVO.setAddress(sourceAsMap.get("address")+"");
                infoVO.setPhone(sourceAsMap.get("phone")+"");
                resultMap.add(infoVO);
            }
        }catch (Exception e) {
            /*
             * 예외처리
             */
            e.printStackTrace();
        }

        Gson gson = new Gson();
        return gson.toJson(resultMap);
    }

    // 지정된 필드에 정확한 텀이 들어있는 문서를 찾을 때 사용한다. Keyword 타입으로 설정된 필드에서만 검색가능
    @RequestMapping(value = "/es/doc/search/term/{matchStr}", method = RequestMethod.GET)
    public String esDocSearchTerm (@PathVariable("matchStr") String matchStr) {
        String indexName = "customer";
        String typeName = "info";

        String keywordFieldName = "phone";
        String searchStr = matchStr;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(keywordFieldName, searchStr));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //searchSourceBuilder.sort(new FieldSortBuilder("answer").order(SortOrder.DESC));

        SearchRequest request = new SearchRequest(indexName);
        request.types(typeName);
        request.source(searchSourceBuilder);


        SearchResponse response = null;
        SearchHits searchHits = null;
        List<InfoVO> resultMap = new ArrayList<>();

        try(RestHighLevelClient client = createConnection();){
            response = client.search(request, RequestOptions.DEFAULT);
            searchHits = response.getHits();
            for( SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                InfoVO infoVO = new InfoVO();
                infoVO.setName(sourceAsMap.get("name")+"");
                infoVO.setAddress(sourceAsMap.get("address")+"");
                infoVO.setPhone(sourceAsMap.get("phone")+"");
                resultMap.add(infoVO);
            }
        }catch (Exception e) {
            e.printStackTrace();
            /*
             * 예외처리
             */
        }

        Gson gson = new Gson();
        return gson.toJson(resultMap);
    }

    // must, mustNot, should, filter 등을 조합해서 쿼리를 구성
    @RequestMapping(value = "/es/doc/search/bool/{matchStr}", method = RequestMethod.GET)
    public String esDocSearchBool (@PathVariable("matchStr") String matchStr) {
        String indexName = "customer";
        String typeName = "info";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("keywordField", "value"))
                .mustNot(QueryBuilders.termQuery("keywordField2", "value2"))
                .should(QueryBuilders.termQuery("keywordField3", "value3"))
                .filter(QueryBuilders.termQuery("keywordField4", "value4")));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //searchSourceBuilder.sort(new FieldSortBuilder("answer").order(SortOrder.DESC));

        SearchRequest request = new SearchRequest(indexName);
        request.types(typeName);
        request.source(searchSourceBuilder);


        SearchResponse response = null;
        SearchHits searchHits = null;
        List<InfoVO> resultMap = new ArrayList<>();

        try(RestHighLevelClient client = createConnection();){
            response = client.search(request, RequestOptions.DEFAULT);
            searchHits = response.getHits();
            for( SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                InfoVO infoVO = new InfoVO();
                infoVO.setName(sourceAsMap.get("name")+"");
                infoVO.setAddress(sourceAsMap.get("address")+"");
                infoVO.setPhone(sourceAsMap.get("phone")+"");
                resultMap.add(infoVO);
            }

        }catch (Exception e) {
            e.printStackTrace();
            /*
             * 예외처리
             */
        }

        Gson gson = new Gson();
        return gson.toJson(resultMap);
    }


    private RestHighLevelClient createConnection() {
        return new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")));
    }
}
