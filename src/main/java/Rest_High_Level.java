import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class Rest_High_Level {

    RestHighLevelClient client;

    Rest_High_Level(){ //CONSTRUCTER
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }
    public void searchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        SearchHits hits = searchResponse.getHits();


        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        SearchHit[] searchHits = hits.getHits();
        for(SearchHit hit : searchHits){
            System.out.println("INDEX   :   "+hit.getIndex());
            System.out.println("TYPE    :   "+hit.getType());
            System.out.println("ID      :   "+hit.getId());
        }


    }
    public void indexRequest(String index, String type, String id, String source) throws IOException {

        IndexRequest request = new IndexRequest(
                index,
                type,
                id);

        request.source(source, XContentType.JSON);

        IndexResponse indexResponse = client.index(request);

    }

    public void getRequest(String index,String type,String id) throws IOException {

        GetRequest getRequest = new GetRequest(
                index,
                type,
                id);

        GetResponse getResponse = client.get(getRequest);
    }

    public void deleteRequest(String index,String type,String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(
                index,
                type,
                id);

        DeleteResponse deleteResponse = client.delete(deleteRequest);

    }
    public void updateRequest(String index,String type,String id,String source) throws IOException {
        UpdateRequest request = new UpdateRequest(
                index,
                type,
                id);

        request.doc(source, XContentType.JSON);
        UpdateResponse updateResponse = client.update(request);

    }
    public boolean bulkRequest(String index,String type,String id,Map source) throws IOException {
        System.out.println(id);
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(index,type, id)
                .source(source));

        BulkResponse bulkResponse = client.bulk(request);

        System.out.println(bulkResponse.buildFailureMessage());
        return bulkResponse.hasFailures();
    }

    public boolean bulkProcessor(String index, String type,String id) throws InterruptedException {


        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                System.out.print("Before BULK Execution ID: "+executionId+"\n");

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.print("After BULK Execution ID: "+executionId
                        +"\nRequest : "+request.getDescription()+"\n Response: "+response.buildFailureMessage()+"\n");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.print("After BULK Execution ID: "+executionId
                        +" Failure"+failure.toString()+"\n");
            }
        };

        BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();

        BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
        builder.setBulkActions(500);
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(0);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));

        IndexRequest one = new IndexRequest("posts", "doc", "1").
                source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts", "doc", "2")
                .source(XContentType.JSON, "title", "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts", "doc", "3")
                .source(XContentType.JSON, "title", "The Future of Federated Search in Elasticsearch");

        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);



        boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
        bulkProcessor.close();
        return terminated;

    }
    public static void main(String[] argv) throws IOException, InterruptedException {
        Rest_High_Level main_client = new Rest_High_Level();

        String index = "first";
        String type = "document";
        String id = "1";

        String source = "{" +
                "\"ip\":\"192.168.1.1\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        //main_client.indexRequest(index,type,id,source);

        //main_client.getRequest(index,type,id);

        //main_client.deleteRequest(index,type,id);

        //main_client.updateRequest(index,type,id,source);

        //logParser logger = new logParser("C:\\Users\\serda\\Desktop\\trkvz-live.access.log.6");

        //main_client.bulkProcessor("asd","asd","asd");

        main_client.searchRequest();

        main_client.client.close();



    }


}

