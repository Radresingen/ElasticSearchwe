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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import java.util.Map;


public class Rest_High_Level {

    private static Rest_High_Level singletonObject = new Rest_High_Level();
    private  RestHighLevelClient client; //Main client for everything
    private  BulkProcessor.Listener listener = null;
    private  BulkProcessor bulkProcessor = null;
    private  BulkProcessor.Builder builder = null;


    private Rest_High_Level(){ //CONSTRUCTER
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }

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
    public void searchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("posts");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        SearchHits hits = searchResponse.getHits();


        boolean clusterName = true;

        Settings settings = Settings.builder()
                .put( "cluster.name", clusterName )
                .put( "client.transport.ignore_cluster_name", true )
                .put( "client.transport.sniff", true )
                .build();


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


    public static void main(String[] argv) throws IOException, InterruptedException {
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

        logParser logger = new logParser("C:\\Users\\serda\\Desktop\\trkvz-live.access.log.6");

        //main_client.bulkProcessor("asd","asd","asd");

        //main_client.searchRequest();

    }


}

