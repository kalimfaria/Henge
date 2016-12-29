package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.scheduler.advancedstela.etp.SupervisorInfo;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * Created by fariakalim on 12/28/16.
 */
public class Latencies {
    private final String USER_AGENT = "Mozilla/5.0";

    private static final Logger LOG = LoggerFactory.getLogger(Latencies.class);
    private String hostname;
    public static String [] supervisors;

    public Latencies() {
        LOG.info("trying to fetch latencies");
        hostname = "Unknown";
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
            GetSupervisors();
        } catch (UnknownHostException ex) {
            System.out.println("Hostname can not be resolved");
        } catch (Exception e) {
            System.out.println("Trying to get supervisor names but failed : " + e.toString());

        }
    }

    private void GetSupervisors () throws  Exception {
        String url = "http://"+hostname+":8080/api/v1/supervisor/summary";

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
        SupervisorInfo.Summaries summaries = gson.fromJson(input, SupervisorInfo.Summaries.class);
        supervisors = new String[summaries.supervisors.length];
        for (int i = 0; i < summaries.supervisors.length; i++) {
            supervisors[i] = summaries.supervisors[i].get_host();
            LOG.info("Supervisor " + supervisors[i]);
        }
        return supervisors;
    }

    public HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>> getLatencies ()  {
        HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>> top_op_latency = new HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>>();
        for (String supervisor: supervisors){
            String url = "http://" + supervisor + ":8000/latencies";
            URL obj = null;
            try {
                obj = new URL(url);
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
            Gson gson = new Gson();
            Info [] infos  = gson.fromJson(response.toString(), Info[].class);
            for (Info info: infos)  {
                LOG.info("Info object " + info.toString());
                HashMap<HashMap<String, String>, ArrayList<Double>> temp = new HashMap<HashMap<String, String>, ArrayList<Double>> ();
                if (top_op_latency.containsKey(info.topology)) {
                    temp = top_op_latency.get(info.topology);
                }
                HashMap <String, String> spout_to_bolts = new HashMap<>();
                spout_to_bolts.put(info.spout,info.sink);
                ArrayList <Double> latencies = new ArrayList<Double>();
                if (temp.containsKey(spout_to_bolts)) {
                    latencies = temp.get(spout_to_bolts);
                }
                latencies.add(info.latency);
                temp.put(spout_to_bolts, latencies);
                top_op_latency.put(info.topology, temp);
            }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // parse the object and make it in the form of Info
        }
       LOG.info("Logging latencies");
        for (String topology: top_op_latency.keySet()) {
            HashMap<HashMap<String, String>, ArrayList<Double>> temp = top_op_latency.get(topology);
            for (HashMap <String, String> spout_bolt : temp.keySet()) {
                LOG.info("topology: " + topology);
                for (String spout: spout_bolt.keySet()) {
                    LOG.info ("spout: " + spout + " sink: " + spout_bolt.get(spout));
                }
                ArrayList <Double> latencies = temp.get(spout_bolt);
                LOG.info("Latencies: " + latencies);
            }
        }
        return top_op_latency;
    }

    public class Info {
        public String topology;
        public String spout;
        public String sink;
        public Double latency;

        @Override
        public String toString(){
            return new String(topology + " " + spout + " " + " " + sink + " " + latency);
        }

    }

}
