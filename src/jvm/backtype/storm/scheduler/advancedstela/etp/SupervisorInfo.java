package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.generated.SupervisorSummary;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;


public class SupervisorInfo {

    private final String USER_AGENT = "Mozilla/5.0";
    String [] supervisors;
    HashMap<String, Info> supervisorsInfo;
    public Queue <HashMap<String, Info>> infoHistory;
    public final int HISTORY_SIZE = 30;
    public final double MAXIMUM_LOAD_PER_MACHINE = 4.0;
    private File util_log;

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorInfo.class);

    public SupervisorInfo() {
        infoHistory = new LinkedList<>();
        supervisorsInfo = new HashMap<String, Info>();
        util_log = new File("/tmp/util.log");
    }

    public void GetSupervisors () throws  Exception {

        String url = "http://zookeepernimbus.storm-cluster.stella.emulab.net:8080/api/v1/supervisor/summary";

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        LOG.info("\nSending 'GET' request to URL : " + url);
        LOG.info("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        supervisors = getSupervisorHosts(response.toString());

    }

    public String [] getSupervisorHosts (String input) {
        Gson gson = new Gson();
        Summaries summaries = gson.fromJson(input, Summaries.class);
        supervisors = new String[summaries.supervisors.length];
        for (int i = 0; i < summaries.supervisors.length; i++) {
            supervisors[i] = summaries.supervisors[i].get_host();
            LOG.info("Supervisor " + supervisors[i]);
        }
        return supervisors;
    }

    public void GetInfo () throws  Exception
    {
        supervisorsInfo = new HashMap<String, Info>();
        for (String supervisor: supervisors){
            String url = "http://" + supervisor + ":8000/info";
            URL obj = new URL(url);

            LOG.info(" Supervisor : " + supervisor + " url " + url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            LOG.info("\nSending 'GET' request to URL : " + url);
            LOG.info("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            // parse the object and make it in the form of Info
            Gson gson = new Gson();
            Info info  = gson.fromJson(response.toString(), Info.class);
            supervisorsInfo.put(supervisor, info);
        }
        insertInfo(supervisorsInfo);
    }

    public void insertInfo (HashMap <String, Info> info) {
        if (infoHistory.size() >= HISTORY_SIZE) {
            // pop the oldest guy and push the new one :)
            infoHistory.remove();
        }
        infoHistory.add(info);
        for (Map.Entry<String, Info> entry: info.entrySet()) {
            writeToFile(util_log, entry.getKey() + " " + entry.getValue().toString() + "\n");

        }
    }


    public boolean areSupervisorsOverUtilized() {
        // if across all of history, in all objects, even one info item says that one supervisor is overloaded, we say
        // that supervisors are overutilised
        boolean [] decisions = new boolean[infoHistory.size()];
        int i = 0;
        for (HashMap <String, Info> history :infoHistory) {
            boolean decision = false;
            for (Map.Entry <String, Info> infoItem : history.entrySet()) {
                if (infoItem.getValue().recentLoad >= MAXIMUM_LOAD_PER_MACHINE) {
                    decision = true;
                }
            }
            decisions[i] = decision;
            i++;
        }
        for (boolean decision: decisions)
            if (!decision){ // this should be false
                return decision;
            }
        return true;
    }

    public boolean GetSupervisorInfo () {
      try {
          this.GetSupervisors();
          this.GetInfo();
      } catch (Exception e)
      {
          System.out.println("Error in getting info about supervisor machines : " + e.toString());
      }
        return areSupervisorsOverUtilized();
    }

    public class Summaries {
        SupervisorSummary [] supervisors;
    }

    public class Info {
        public Double recentLoad;
        public Double minLoad;
        public Double fiveMinsLoad;
        public Double freeMem;
        public Double usedMemory;
        public Double usedMemPercent;
        public Long time;

        @Override
        public String toString(){
            return new String(recentLoad + " " + minLoad + " " + " " + fiveMinsLoad + " " + freeMem + " " + usedMemory + " " +
            usedMemPercent + " " + time);
        }

    }
    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
        } catch (IOException ex) {
            LOG.info("error! writing to file {}", ex);
        }
    }
}
