package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.scheduler.*;
import backtype.storm.scheduler.advancedstela.Helpers;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalState {

    private final String USER_AGENT = "Mozilla/5.0";

    private Map config;
    private NimbusClient nimbusClient;
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);
    /* Topology schedules which store the schedule state of the topology. */
    private HashMap<String, TopologySchedule> topologySchedules;
    private boolean isClusterOverUtilized;
    private File capacityLog;
    private Helpers helper;


    public void setClusterUtilization(boolean isOverUtilized) {
        isClusterOverUtilized = isOverUtilized;
    }

    public boolean isClusterUtilization() {
        LOG.info("Cluster utilization {} ", isClusterOverUtilized);
        return isClusterOverUtilized;
    }

    public GlobalState(Map conf) {
        config = conf;
        topologySchedules = new HashMap<String, TopologySchedule>();
        capacityLog = new File("/tmp/capacity.log");
        isClusterOverUtilized = false;
        helper = new Helpers();

    }

    public HashMap<String, TopologySchedule> getTopologySchedules() {
        return topologySchedules;
    }

    public void setCapacities(HashMap<String, backtype.storm.scheduler.advancedstela.slo.Topology> Topologies) {

        String url = "http://zookeepernimbus:8080/api/v1/topology/";
        for (Map.Entry<String, Topology> topology : Topologies.entrySet()) {
            ///api/v1/topology/:id
            String topologyURL = url + topology.getKey();
            LOG.info("topologyURL {}", topologyURL);
            try {
                URL obj = new URL(topologyURL);
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

                JSONObject object = new JSONObject(response.toString());
                String[] keys = JSONObject.getNames(object);

                TopologySchedule topologySchedule = topologySchedules.get(topology.getKey());
                HashMap<String, Component> etpComponents = topologySchedule.getComponents();

                for (String key : keys) {
                    if (key.equals("bolts")) {
                        JSONArray value = (JSONArray) object.get(key);
                        for (int i = 0; i < value.length(); i++) {
                            JSONObject jsonObj = (JSONObject) value.get(i);
                            etpComponents.get(jsonObj.get("boltId")).setCapacity(Double.parseDouble((String) jsonObj.get("capacity")));
                        }
                    }
                }

                for (Map.Entry<String, Component> component: etpComponents.entrySet()) {
                    LOG.info("Topology {} Component {} Capacity {}", topology.getKey(), component.getKey(), component.getValue().getCapacity());
                    helper.writeToFile(capacityLog, topology.getKey() + " " + component.getKey() + " " + component.getValue().getCapacity() + " " + System.currentTimeMillis() + "\n");
                }

            } catch (Exception e) {
                LOG.info(e.toString());
            }
        }
    }

    public boolean collect(Cluster cluster, Topologies topologies) {
        SupervisorInfo info = new SupervisorInfo();
        setClusterUtilization(info.GetSupervisorInfo());
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));
            } catch (TTransportException e) {
                e.printStackTrace();
                return false;
            }
        }
        int clusterSize  = info.getSupervisorSize();
        int supervisorSize = cluster.getSupervisors().size();
        populateAssignmentForTopologies(cluster, topologies);
        LOG.info("Cluster size {} supervisor Size {}", clusterSize, supervisorSize);
        if (supervisorSize < clusterSize) return false;
        return true;

    }



    private void populateAssignmentForTopologies(Cluster cluster, Topologies topologies) {
        populateTopologyInformation();
        populateExecutorsForTopologyComponents(topologies);
        populateTopologyWorkerSlotToExecutors(cluster);
    }

    private void populateTopologyInformation() {
        try {
            List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();

            for (TopologySummary topologySummary : topologies) {
                String id = topologySummary.get_id();
                StormTopology topology = nimbusClient.getClient().getTopology(id);
                TopologySchedule topologySchedule = new TopologySchedule(id);
                TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(topologySummary.get_id());

                try {
                    addSpoutsAndBolts(topology, topologySchedule, topologyInfo);

                    topologySchedules.put(id, topologySchedule);
                } catch (Exception e) {
                    LOG.info("exception while trying to add spouts and bolts : {}", e.toString());
                    LOG.info("Logging the topology information that we just got");
                    for (ExecutorSummary execSummary : topologyInfo.get_executors()) {
                        LOG.info("Component {} host {} port {}", execSummary.get_component_id(), execSummary.get_host(), execSummary.get_port());
                    }
                }
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }



    private void populateExecutorsForTopologyComponents(Topologies topologies) {
        try {
            for (TopologyDetails topologyDetails : topologies.getTopologies()) {

                TopologyInfo topologyInformation = nimbusClient.getClient().getTopologyInfo(topologyDetails.getId());
                TopologySchedule topologySchedule = topologySchedules.get(topologyDetails.getId());

                for (Map.Entry<ExecutorDetails, String> executorToComponent :
                        topologyDetails.getExecutorToComponent().entrySet()) {

                    Component component = topologySchedule.getComponents().get(executorToComponent.getValue());

                    if (component != null) {
                        component.addExecutor(executorToComponent.getKey());
                        topologySchedule.addExecutorToComponent(executorToComponent.getKey(), component.getId());
                    }
                }

                for (ExecutorSummary executorSummary : topologyInformation.get_executors()) {

                    Component component = topologySchedule.getComponents().get(executorSummary.get_component_id());
                    if (component != null) {
                        component.addExecutorSummary(executorSummary);
                    }
                }

            }
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void populateTopologyWorkerSlotToExecutors(Cluster cluster) {
        for (String topologyId : topologySchedules.keySet()) {
            SchedulerAssignment schedulerAssignment = cluster.getAssignmentById(topologyId);
            if (schedulerAssignment != null) {
                TopologySchedule topologySchedule = topologySchedules.get(topologyId);
                Map<ExecutorDetails, WorkerSlot> executorToSlot = schedulerAssignment.getExecutorToSlot();
                for (Map.Entry<ExecutorDetails, WorkerSlot> mapping : executorToSlot.entrySet()) {
                    topologySchedule.addAssignment(mapping.getValue(), mapping.getKey());
                }
            }
        }
    }

    private void addSpoutsAndBolts(StormTopology stormTopology, TopologySchedule topologySchedule, TopologyInfo topologyInfo) throws TException {
        HashMap<String, Integer> parallelism_hints = new HashMap<>();

        List<ExecutorSummary> execSummary = topologyInfo.get_executors();
        for (int i = 0; i < execSummary.size(); i++) {
            if (parallelism_hints.containsKey(execSummary.get(i).get_component_id()))
                parallelism_hints.put(execSummary.get(i).get_component_id(), parallelism_hints.get(execSummary.get(i).get_component_id()) + 1);
            else
                parallelism_hints.put(execSummary.get(i).get_component_id(), 1);
        }

        if (topologyInfo.get_executors().size() == 0) {

            for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
                if (!spout.getKey().matches("(__).*")) {
                    topologySchedule.addComponents(spout.getKey(), new Component(spout.getKey(),
                            spout.getValue().get_common().get_parallelism_hint()));
                }
            }

            for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
                if (!bolt.getKey().matches("(__).*")) {
                    topologySchedule.addComponents(bolt.getKey(), new Component(bolt.getKey(),
                            bolt.getValue().get_common().get_parallelism_hint()));
                }
            }

        } else {
            for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
                if (!spout.getKey().matches("(__).*")) {
                    topologySchedule.addComponents(spout.getKey(), new Component(spout.getKey(),
                            parallelism_hints.get(spout.getKey())));

                }
            }

            for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
                if (!bolt.getKey().matches("(__).*")) {
                    topologySchedule.addComponents(bolt.getKey(), new Component(bolt.getKey(),
                            parallelism_hints.get(bolt.getKey())));
                }
            }
        }
    }


}