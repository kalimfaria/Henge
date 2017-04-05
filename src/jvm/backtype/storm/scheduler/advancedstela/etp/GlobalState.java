package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.scheduler.*;
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
    private static final String ALL_TIME = ":all-time";
    private static final String TEN_MINS = "600";
    private static final String DEFAULT = "default";
    private final String USER_AGENT = "Mozilla/5.0";

    private Map config;
    //String hostname;
    private NimbusClient nimbusClient;
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);
    //private File latency_log;
    /* Topology schedules which store the schedule state of the topology. */
    private HashMap<String, TopologySchedule> topologySchedules;
    private boolean isClusterOverUtilized;
    private File capacityLog;


    public void setClusterUtilization(boolean isOverUtilized) {
        isClusterOverUtilized = isOverUtilized;
    }

    public boolean isClusterUtilization() {
      //  LOG.info("Cluster utilization {} ", isClusterOverUtilized);
        return isClusterOverUtilized;
    }

    /* Supervisor to node mapping. */
    private HashMap<String, Node> supervisorToNode;

    public GlobalState(Map conf) {
        config = conf;
        topologySchedules = new HashMap<String, TopologySchedule>();
        supervisorToNode = new HashMap<String, Node>();
      //  latency_log = new File("/tmp/latency.log");
        capacityLog = new File("/tmp/capacity.log");
        isClusterOverUtilized = false;
   /*     hostname = "Unknown";
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
            String [] broken = hostname.split(".");
            hostname = broken[0];
        } catch (UnknownHostException ex) {
            System.out.println("Hostname can not be resolved");
        } */
    }

    public HashMap<String, TopologySchedule> getTopologySchedules() {
        return topologySchedules;
    }

    public void setCapacities(HashMap<String, backtype.storm.scheduler.advancedstela.slo.Topology> Topologies) {

        String url = "http://zookeepernimbus:8080/api/v1/topology/";
        for (Map.Entry<String, Topology> topology : Topologies.entrySet()) {
            ///api/v1/topology/:id
            String topologyURL = url + topology.getKey();
          //  LOG.info("topologyURL {}", topologyURL);
            try {
                URL obj = new URL(topologyURL);
                HttpURLConnection con = (HttpURLConnection) obj.openConnection();

                // optional default is GET
                con.setRequestMethod("GET");

                //add request header
                con.setRequestProperty("User-Agent", USER_AGENT);

                int responseCode = con.getResponseCode();
              ///  LOG.info("\nSending 'GET' request to URL : " + url);
              ///  LOG.info("Response Code : " + responseCode);

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
                  //  LOG.info("Topology {} Component {} Capacity {}", topology.getKey(), component.getKey(), component.getValue().getCapacity());
                    writeToFile(capacityLog, topology.getKey() + " " + component.getKey() + " " + component.getValue().getCapacity() + " " + System.currentTimeMillis() + "\n");
                }

            } catch (Exception e) {
                LOG.info(e.toString());
            }
        }
    }

    public void collect(Cluster cluster, Topologies topologies) {
        SupervisorInfo info = new SupervisorInfo();
        setClusterUtilization(info.GetSupervisorInfo());
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));
            } catch (TTransportException e) {
                e.printStackTrace();
                return;
            }
        }
        populateNodeToExecutorMapping(cluster);
        populateAssignmentForTopologies(cluster, topologies);
    }

    private void populateNodeToExecutorMapping(Cluster cluster) {
        for (Map.Entry<String, SupervisorDetails> entry : cluster.getSupervisors().entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorDetails supervisorDetails = cluster.getSupervisors().get(supervisorId);
            Node node = new Node(supervisorDetails, cluster.getAssignableSlots(supervisorDetails));
            supervisorToNode.put(supervisorId, node);
        }

        for (Map.Entry<String, SchedulerAssignment> entry : cluster.getAssignments().entrySet()) {
            for (Map.Entry<ExecutorDetails, WorkerSlot> executor : entry.getValue().getExecutorToSlot().entrySet()) {
                String nodeId = executor.getValue().getNodeId();
                if (supervisorToNode.containsKey(nodeId)) {
                    Node node = supervisorToNode.get(nodeId);
                    if (node.slotsToExecutors.containsKey(executor.getValue())) {
                        node.slotsToExecutors.get(executor.getValue()).add(executor.getKey());
                        node.executors.add(executor.getKey());
                    } else {
                       // LOG.error("ERROR: should have node {} should have worker: {}", executor.getValue().getNodeId(),                         executor.getValue());
                    }
                } else {
                    // LOG.error("ERROR: should have node {}", executor.getValue().getNodeId());
                }
            }
        }
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
                TopologySchedule topologySchedule = new TopologySchedule(id, topologySummary.get_num_workers());
                TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(topologySummary.get_id());

                try {
                    addSpoutsAndBolts(topology, topologySchedule, topologyInfo);
                    constructTopologyGraph(topology, topologySchedule);
                    topologySchedules.put(id, topologySchedule);
                } catch (Exception e) {
                   // LOG.info("exception while trying to add spouts and bolts : {}", e.toString());
                   // LOG.info("Logging the topology information that we just got");
                    //for (ExecutorSummary execSummary : topologyInfo.get_executors()) {
                     //   LOG.info("Component {} host {} port {}", execSummary.get_component_id(), execSummary.get_host(), execSummary.get_port());
                    //}
                    e.printStackTrace();
                }
            }
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

    private void populateExecutorsForTopologyComponents(Topologies topologies) {
        try {
            HashMap<String, Double> temporaryExecuteLatency = new HashMap<>();
            HashMap<String, Double> temporaryExecuteLatency_10Mins = new HashMap<>();
            HashMap<String, Double> temporaryProcessLatency = new HashMap<>();
            HashMap<String, Double> temporaryCompleteLatency = new HashMap<>();

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
                // End of old implementation
                //// From Observer -- why are we doing things twice?

                for (ExecutorSummary executorSummary : topologyInformation.get_executors()) { // SHALL WE PUT THESE TWO LOOPS TOGETHER?
                    String componentId = executorSummary.get_component_id();
                    //   System.out.println("Component Id: " + componentId);
                    Component component = topologySchedule.getComponents().get(componentId);

                    if (component == null) {
                        continue;
                    }
                    ExecutorStats stats = executorSummary.get_stats();
                    if (stats == null) {
                        continue;
                    }

                    if (stats.get_specific().is_set_spout()) {
                        SpoutStats spoutStats = stats.get_specific().get_spout();
                        if (spoutStats.is_set_complete_ms_avg()) {
                            Map<String, Map<String, Double>> complete_msg_avg = spoutStats.get_complete_ms_avg();

                            Map<String, Double> statValues = complete_msg_avg.get(ALL_TIME);

                            for (String key : statValues.keySet()) {
                                if (DEFAULT.equals(key)) {
                                    if (!temporaryCompleteLatency.containsKey(componentId)) {
                                        temporaryCompleteLatency.put(componentId, 0.0);
                                    }
                                    temporaryCompleteLatency.put(componentId, temporaryCompleteLatency.get(componentId) +
                                            statValues.get(key).doubleValue());
                                }
                            }

                        }
                    } else if (stats.get_specific().is_set_bolt()) {
                        BoltStats boltStats = stats.get_specific().get_bolt();
                        if (boltStats.is_set_execute_ms_avg()) {
                            Map<String, Map<GlobalStreamId, Double>> execute_msg_avg = boltStats.get_execute_ms_avg();


                            Map<GlobalStreamId, Double> statValues = execute_msg_avg.get(ALL_TIME);
                            Map<GlobalStreamId, Double> statValues_10Mins = execute_msg_avg.get(TEN_MINS);

                            for (GlobalStreamId key : statValues.keySet()) {
                                if (DEFAULT.equals(key.get_streamId())) {
                                    if (!temporaryExecuteLatency.containsKey(componentId)) {
                                        temporaryExecuteLatency.put(componentId, 0.0);
                                    }
                                    temporaryExecuteLatency.put(componentId, temporaryExecuteLatency.get(componentId) +
                                            statValues.get(key).doubleValue());

                                }
                            }


                            for (GlobalStreamId key : statValues_10Mins.keySet()) {
                                if (DEFAULT.equals(key.get_streamId())) {
                                    if (!temporaryExecuteLatency_10Mins.containsKey(componentId)) {
                                        temporaryExecuteLatency_10Mins.put(componentId, 0.0);
                                    }
                                    temporaryExecuteLatency_10Mins.put(componentId, temporaryExecuteLatency_10Mins.get(componentId) +
                                            statValues_10Mins.get(key).doubleValue());

                                }
                            }
                        }

                        if (boltStats.is_set_process_ms_avg()) {
                            Map<String, Map<GlobalStreamId, Double>> process_msg_avg = boltStats.get_process_ms_avg();
                            Map<GlobalStreamId, Double> statValues = process_msg_avg.get(ALL_TIME);

                            for (GlobalStreamId key : statValues.keySet()) {
                                if (DEFAULT.equals(key.get_streamId())) {
                                    if (!temporaryProcessLatency.containsKey(componentId)) {
                                        temporaryProcessLatency.put(componentId, 0.0);
                                    }
                                    temporaryProcessLatency.put(componentId, temporaryProcessLatency.get(componentId) +
                                            statValues.get(key).doubleValue());
                                }
                            }
                        }
                        /*(defn compute-executor-capacity
                          [^ExecutorSummary e]
                          (let [stats (.get_stats e)
                                stats (if stats
                                        (-> stats
                                            (aggregate-sink-stats true)
                                            (aggregate-sink-streams)
                                            swap-map-order
                                            (get "600")))
                                uptime (nil-to-zero (.get_uptime_secs e))
                                window (if (< uptime 600) uptime 600) //executorSummary.get_uptime_secs()
                                executed (-> stats :executed nil-to-zero)
                                latency (-> stats :execute-latencies nil-to-zero)]
                            (if (> window 0)
                              (div (* executed latency) (* 1000 window)))))*/
                    }
                }


                for (String componentId : topologySchedule.getComponents().keySet()) {
                    Component component = topologySchedule.getComponents().get(componentId);
                    if (temporaryProcessLatency.containsKey(componentId))
                        component.setProcessLatency(temporaryProcessLatency.get(componentId) / (double) component.getParallelism());

                    if (temporaryExecuteLatency.containsKey(componentId)) {
                        component.setExecuteLatency(temporaryExecuteLatency.get(componentId) / (double) component.getParallelism());
                    }

                    if (temporaryExecuteLatency_10Mins.containsKey(componentId)) {
                        component.setExecuteLatency10mins(temporaryExecuteLatency_10Mins.get(componentId) / (double) component.getParallelism());
                    }

                    if (temporaryCompleteLatency.containsKey(componentId))
                        component.setCompleteLatency(temporaryCompleteLatency.get(componentId) / (double) component.getParallelism());

                   /// writeToFile(latency_log, topologySchedule.getId() + "," + componentId + "," + component.getCompleteLatency() + "," + component.getExecuteLatency() + "," + component.getProcessLatency() + "," + System.currentTimeMillis() + "\n");
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

    private void constructTopologyGraph(StormTopology topology, TopologySchedule topologySchedule) {
        for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                Component component = topologySchedule.getComponents().get(bolt.getKey());

                for (Map.Entry<GlobalStreamId, Grouping> parent : bolt.getValue().get_common().get_inputs().entrySet()) {
                    String parentId = parent.getKey().get_componentId();

                    if (topologySchedule.getComponents().get(parentId) == null) {
                        topologySchedule.getComponents().get(parentId).addChild(component.getId());
                    } else {
                        topologySchedule.getComponents().get(parentId).addChild(component.getId());
                    }
                    component.addParent(parentId);
                }
            }
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