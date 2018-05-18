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

    public static void fileParser(String filePath) throws IOException, InterruptedException {

        Rest_High_Level rhl = new Rest_High_Level();
        Map<String,Object> jsonMap = new HashMap<>();

        File file = new File(filePath);
        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        int i=0;
        while ((st = br.readLine()) != null ){
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
                        jsonMap.put("message",tmp);
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
                            jsonMap.put("status",t);
                            flag3++;
                            break;
                        case 2://BYTE
                            jsonMap.put("byte",t);
                            flag3++;
                            break;
                    }
                    if(flag3 == 3){
                        //rhl.bulkProcessor("first","document",Integer.toString(i),jsonMap);
                        flag3=0;
                        break;
                    }

                }
            }
            i++;
        }

            //BULK REQUEST İLE BULK PROCESSOR FARKLI FONKSİYONLARA AYIR

            //WHİLE İÇİNDE REQUEST FONKSİYONU ÇAĞIRIP RETURN OLARAK BULKREQUEST AL BUNU BİR
            //ARRAYLİSTE SAKLA ARDINDAN ARRAYLİSTİ TEK SEFERDE BULKPROCESSOR FONKSİYONUNA VER


            //System.out.println(st);



    }


}
