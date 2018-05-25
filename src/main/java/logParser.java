import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class logParser {
    String ipAddress;
    String date;
    String httpRequestType;
    String httpRequest;
    String httpStatus;
    String dataVolumeinByte;

    String filePath;

    logParser(String filePath) throws IOException {
        this.filePath = filePath;
        try {
            fileParser(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private static String dateParser(String date){
        int month=0;
        switch (date.substring(2,6)){
            case "Jan":
                        month = 1;
                        break;
            case "Feb":
                        month = 2;
                        break;
            case "Mar":
                        month = 3;
                        break;
            case "Apr":
                        month = 4;
                        break;
            case "May":
                        month = 5;
                        break;
            case "Jun":
                        month = 6;
                        break;
            case "Jul":
                        month = 7;
                         break;
            case "Aug":
                        month = 8;
                        break;
            case "Sep":
                        month = 9;
                        break;
            case "Oct":
                        month = 10;
                        break;
            case "Nov":
                        month = 11;
                        break;
            case "Dec":
                        month = 12;
                        break;

        }

        String convertedDate = date.substring(6,11)+"-"+((month < 10) ? ("0"+Integer.toString(month)) : (Integer.toString(month)))
                +"-"+date.substring(0,3) +"T"+date.substring(11,20)+"Z";
        System.out.printnln(convertedDate);
        return convertedDate;
    }
    public static void fileParser(String filePath) throws IOException, InterruptedException {

        Rest_High_Level mainClient = Rest_High_Level.getInstance();

        mainClient.createBulkProcessorListener();

        Map<String,Object> jsonMap = new HashMap<>();

        jsonMap.put("ip","a");
        jsonMap.put("date","a");
        jsonMap.put("httpRequest","a");
        jsonMap.put("status","a");
        jsonMap.put("byte","a");

        File file = new File(filePath);
        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        int i=0;
        while ((st = br.readLine()) != null & i<10){
            String[] tokens = st.split(" ");
            String tmp="";
            boolean flag1 = false;
            boolean flag2 = false;
            int flag3 = 0;
            int token=0;
            for (String t : tokens){
                if(t.substring(0,1).compareTo("[") == 0){
                    tmp += t.substring(1);
                    flag1 = true;
                }
                else if(t.substring(t.length()-1,t.length()).compareTo("]") == 0 || flag1){
                    if(t.substring(t.length()-1,t.length()).compareTo("]") == 0){
                        tmp += t.substring(0,t.length()-1);
                        //System.out.println(tmp);
                        jsonMap.put("date",tmp);
                        tmp="";
                        flag1=false;
                    }
                    else{
                        tmp += t;
                    }
                }
                else if(t.substring(0,1).compareTo("\"") == 0 ){
                    tmp += t.substring(1);
                    flag2 = true;
                }
                else if(t.substring(t.length()-1,t.length()).compareTo("\"") == 0 || flag2){
                    if(t.substring(t.length()-1,t.length()).compareTo("\"") == 0){
                        tmp += t.substring(0,t.length()-1);
                        jsonMap.put("httpRequest",tmp);
                        tmp = "";
                        flag2 = false;
                    }
                    else if(flag2){
                        tmp += t;
                    }
                }
                else if(t.compareTo("-") == 0){
                    continue;
                }
                else{
                    switch(flag3){
                        case 0://IP SECTION
                            jsonMap.put("ip",t);
                            flag3++;
                            break;
                        case 1://STATUS SECTION
                            jsonMap.put("status",Integer.parseInt(t));
                            flag3++;
                            break;
                        case 2://BYTE
                            jsonMap.put("byte",Integer.parseInt(t));
                            flag3++;
                            break;
                    }
                    /*if(flag3 == 3){
                        IndexRequest request = new IndexRequest("posts", "doc", Integer.toString(i))
                                .source(jsonMap);
                        mainClient.addRequestToBulkProcessor(request);
                        flag3=0;
                        break;
                    }*/

                }
            }
            i++;
        }


        mainClient.flushBulkProcessor();
        mainClient.close();
            //System.out.println(st);



    }


}
