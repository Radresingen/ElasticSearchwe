import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
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
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import java.util.Map;


public class Rest_High_Level {

    private static Rest_High_Level singletonObject = new Rest_High_Level();
    private RestHighLevelClient client;
    private BulkProcessor.Listener listener = null;
    private BulkProcessor bulkProcessor = null;
    private BulkProcessor.Builder builder = null;


    private Rest_High_Level(){ //CONSTRUCTER
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }

    /*public void putMapping(String index,String type,String source,boolean flag){
        //if flag false method will add new type while creating an index
        //if flag true method will add a new type to an existing index

        if(flag){
            client.admin().indices().preparePutMapping(index)
                    .setType(type)
                    .setSource("{\n" +
                            "  \"properties\": {\n" +
                            "    \"name\": {\n" +
                            "      \"type\": \"text\"\n" +
                            "    }\n" +
                            "  }\n" +
                            "}", XContentType.JSON)
                    .get();
        }
        else{

        }
    }*/
    public void close(){
        bulkProcessor.close();

    }
    public static Rest_High_Level getInstance(){
        return singletonObject;
    }

    public  boolean createBulkProcessorListener(){
        if(listener == null){
            listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                /*int numberOfActions = request.numberOfActions();
                System.out.print("Before BULK Execution ID: "+executionId+"\n");*/

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                /*System.out.print("After BULK Execution ID: "+executionId
                        +"\nRequest : "+request.getDescription()+"\n Response: "+response.buildFailureMessage()+"\n");*/
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    System.out.print("After BULK Execution ID: "+executionId
                            +" Failure"+failure.toString()+"\n");

                }
            };
            createBulkProcessor();
            return true;
        }

       return false;
    }
    public  boolean createBulkProcessor(){
        if(bulkProcessor == null){
            bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();
            createbulkProcessorBuilder();
            return true;
        }
        return false;
    }

    public boolean createbulkProcessorBuilder(){
        if(builder == null){
            builder = BulkProcessor.builder(client::bulkAsync, listener);
            builder.setBulkActions(-1);
            builder.setBulkSize(new ByteSizeValue(15, ByteSizeUnit.MB));
            builder.setConcurrentRequests(1);
            builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
            return true;
        }
        return false;
    }
    public boolean addRequestToBulkProcessor(Object request){
        if(request instanceof IndexRequest){
            //System.out.println("Index Request");
            bulkProcessor.add((IndexRequest)request);
            return true;
        }
        else if(request instanceof DeleteRequest){
            //System.out.println("Delete Request");
            bulkProcessor.add((DeleteRequest)request);
            return true;
        }
        else if(request instanceof UpdateRequest){
            //System.out.println("Update Request");
            bulkProcessor.add((UpdateRequest)request);
            return true;
        }
        else{
            return false;
        }
    }

    public boolean flushBulkProcessor(){
        bulkProcessor.flush();
        return true;
    }
    public void searchRequest(String index,String type) throws IOException {
        //if index is null then search is not specified.
        SearchRequest searchRequest;
        if(index == null)
             searchRequest = new SearchRequest();
        else
            searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());


        if(type != null)
            searchRequest.types(type);

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2000);

        /*Adding time limit for the query execution time

        searchSourceBuilder.timeout(new TimeValue(60,TimeUnit.SECONDS));

        */


        searchRequest.source(searchSourceBuilder);


        /* SYNCHRONOUS EXECUTION

        SearchResponse searchResponse = client.search(searchRequest);

        */

        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                RestStatus status = searchResponse.status();
                TimeValue took = searchResponse.getTook();
                Boolean terminatedEarly = searchResponse.isTerminatedEarly();
                boolean timedOut = searchResponse.isTimedOut();

                searchToPrint(searchResponse);
            }

            @Override
            public void onFailure(Exception e) {

            }
        };

        client.searchAsync(searchRequest,listener);

    }
    private void searchToPrint(SearchResponse searchResponse){
        String sourceAsString;
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();
        SearchHit[] searchHits = hits.getHits();

        for(SearchHit hit : searchHits){
            System.out.println("index: "+hit.getIndex());
            System.out.println("type: "+hit.getType());
            System.out.println("id: "+hit.getId());
            System.out.println("score: "+hit.getScore());

            sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }
    public boolean createIndexRequest(String index,String type,String mapping) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index);
        if(mapping != null){
            request.mapping(type,
                    "  {\n" +
                            "    \""+type+"\": {\n" +
                            "      \"properties\": {\n" +
                            "        \"ip\": {\n" +
                            "          \"type\": \"ip\"\n" +
                            "        },\n"+
                            "        \"date\": {\n" +
                            "          \"type\": \"date\"\n" +
                            "        },\n" +
                            "        \"httpRequest\": {\n" +
                            "          \"type\": \"text\"\n" +
                            "        },\n" +
                            "        \"status\": {\n" +
                            "          \"type\": \"short\"\n" +
                            "        },\n" +
                            "        \"byte\": {\n" +
                            "          \"type\": \"short\"\n"+
                            "        }\n" +
                            "      }\n" +
                            "    }\n" +
                            "  }",
                    XContentType.JSON);
        }
        CreateIndexResponse createIndexResponse = client.indices().create(request);
        return createIndexResponse.isAcknowledged();
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


    public static void main(String[] argv) throws IOException, InterruptedException {
        Rest_High_Level mainClient = Rest_High_Level.getInstance();

        String index = "first";
        String type = "document";
        String id = "1";

        String source = "{" +
                "\"ip\":\"192.168.1.1\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        logParser logger = new logParser("C:\\Users\\serda\\Desktop\\trkvz-live.access.log.6");
        /*if(mainClient.createIndexRequest("posts","doc","a"))
            System.out.println("OK");
*/

        //mainClient.searchRequest("posts","doc");
        //mainClient.close();

    }


}

