package backtype.storm.scheduler.advancedstela.etp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

public class SupervisorInfo {

    private final String USER_AGENT = "Mozilla/5.0";
    String [] supervisors;
    HashMap<String, Info> supInfo;

    public void GetSupervisors () throws  Exception {

        String url = "http://zookeepernimbus.storm-cluster.stella.emulab.net:8080/api/v1/supervisor/summary";

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        //print result
        System.out.println(response.toString());
       // parse to JSON
        // parse and place in string array
        // refresh each time
    }
     /*
     {
         "supervisors": [
         {
             "id": "21dee14c-b2c3-4a41-a7a0-dff6fcf4fa34",
                 "host": "pc427.emulab.net",
                 "uptime": "36m 40s",
                 "slotsTotal": 4,
                 "slotsUsed": 2,
                 "version": "0.10.1-SNAPSHOT"
         },
         {
             "id": "6c741fe4-3cf8-4153-b363-0ab4d21d3b99",
                 "host": "pc557.emulab.net",
                 "uptime": "36m 37s",
                 "slotsTotal": 4,
                 "slotsUsed": 1,
                 "version": "0.10.1-SNAPSHOT"
         },
         {
             "id": "d81c5f22-ee13-4ba7-bdc7-9b39192c6666",
                 "host": "pc436.emulab.net",
                 "uptime": "36m 46s",
                 "slotsTotal": 4,
                 "slotsUsed": 1,
                 "version": "0.10.1-SNAPSHOT"
         },
         {
             "id": "7c0f6b70-8438-4387-988f-ba08bdcc4f17",
                 "host": "pc538.emulab.net",
                 "uptime": "36m 54s",
                 "slotsTotal": 4,
                 "slotsUsed": 1,
                 "version": "0.10.1-SNAPSHOT"
         },
         {
             "id": "675925af-c7b9-4e4e-8c7b-ba11f618ca97",
                 "host": "pc553.emulab.net",
                 "uptime": "36m 58s",
                 "slotsTotal": 4,
                 "slotsUsed": 1,
                 "version": "0.10.1-SNAPSHOT"
         }
         ]
     } */

    public void GetInfo () throws  Exception
    {
        for (String supervisor: supervisors){
            String url = "http://" + supervisor + ":8000/info";
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            System.out.println(response.toString());
            // parse the object and make it in the form of Info
        }
    }

    public void GetSupervisorInfo () {
      try {
          this.GetSupervisors();
          this.GetInfo();
      } catch (Exception e)
      {
          System.out.println("Error in getting info about supervisor machines : " + e.toString());
      }
    }

    public class Info {
        Double recentLoad;
        Double minuteLoad;
        Double fiveMinsLoad;
        Double freeMem;
        Double usedMemory;
        Double usedMemPercent;
        Long time;
    }
}
