package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.scheduler.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalState {
    private static final String ALL_TIME = ":all-time";
    private static final String TEN_MINS = "600";
    private static final String DEFAULT = "default";

    private Map config;
    private NimbusClient nimbusClient;
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);
    // private File advanced_scheduling_log;
    private File latency_log;
    /* Topology schedules which store the schedule state of the topology. */
    private HashMap<String, TopologySchedule> topologySchedules;

    /* Supervisor to node mapping. */
    private HashMap<String, Node> supervisorToNode;
    private Long lastTime;
    private SupervisorInfo supervisorInfo;

    public GlobalState(Map conf) {
        config = conf;
        topologySchedules = new HashMap<String, TopologySchedule>();
        supervisorToNode = new HashMap<String, Node>();
        latency_log = new File("/tmp/latency.log");
       // lastTime = System.currentTimeMillis();
    }

    public HashMap<String, TopologySchedule> getTopologySchedules() {
        return topologySchedules;
    }


    public void setCapacities(HashMap<String, backtype.storm.scheduler.advancedstela.slo.Topology> Topologies)
    {
       // Long now = System.currentTimeMillis();
        for (String ID: Topologies.keySet())
        {
            System.out.println("Topology: " + ID);
            TopologySchedule topologySchedule = topologySchedules.get(ID);
            HashMap <String, Component> etpComponents = topologySchedule.getComponents();
            HashMap <String, backtype.storm.scheduler.advancedstela.slo.Component> sloComponents = Topologies.get(ID).getAllComponents();

            for (String etpCompID : etpComponents.keySet())
            {

//                if (now > (lastTime + 60000)) {
                    double execute_latency = etpComponents.get(etpCompID).getExecuteLatency10mins();

                    System.out.println(" Component ID: " + etpCompID + " Execute Latency: " + execute_latency);

                    double totalExecutedTuples = 0.0;
                    for (double val : sloComponents.get(etpCompID).getCurrentExecuted_10MINS().values()) {
                        totalExecutedTuples = totalExecutedTuples + val;
                    }

                    System.out.println(" Component ID: " + etpCompID + " Total Executed Tuples: " + totalExecutedTuples + " Average Executed Tuples: " + totalExecutedTuples / sloComponents.get(etpCompID).getParallelism());

                   // System.out.println("Now: " + now);
                   // System.out.println("Last Time: " + lastTime);
                    totalExecutedTuples = totalExecutedTuples / sloComponents.get(etpCompID).getParallelism();

                   // System.out.println("Difference:  " + (now - lastTime));
                    double capacity = (totalExecutedTuples * execute_latency) / 600000;
                    etpComponents.get(etpCompID).setCapacity(capacity);

                    System.out.println("Topology: " + ID + " Component ID: " + etpCompID + " Capacity: " + capacity);
                //}

            }
         //   lastTime = now;
        }
    }

    public HashMap<String, Node> getSupervisorToNode() {
        return supervisorToNode;
    }

    public void collect(Cluster cluster, Topologies topologies) {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));
            } catch (TTransportException e) {
                e.printStackTrace();
                return;
            }
        }
      //  supervisorInfo.GetSupervisorInfo(); // TODO later complete implementation :)
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
                        LOG.error("ERROR: should have node {} should have worker: {}", executor.getValue().getNodeId(),
                                executor.getValue());
                    }
                } else {
                    LOG.error("ERROR: should have node {}", executor.getValue().getNodeId());
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

                addSpoutsAndBolts(topology, topologySchedule, topologyInfo);
                constructTopologyGraph(topology, topologySchedule);

                topologySchedules.put(id, topologySchedule);
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
                         /*   for (String key : complete_msg_avg.keySet()) {
                                System.out.println("Key: " + key);
                                System.out.println("What's in the complete_msg_avg.keySet() map?");
                                for (String inner_key : complete_msg_avg.get(key).keySet()) {
                                    System.out.println("The inner key : " + inner_key);
                                    System.out.println("The inner value : " + complete_msg_avg.get(key).get(inner_key).toString());
                                      }
                            } */
                            // been through the loop

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
                          //  System.out.println("In temporaryCompleteLatency.get("+ componentId+ "):  "  + temporaryCompleteLatency.get(componentId));
                        }
                    } else if (stats.get_specific().is_set_bolt()) {
                        BoltStats boltStats = stats.get_specific().get_bolt();
                        if (boltStats.is_set_execute_ms_avg()) {
                            Map<String, Map<GlobalStreamId, Double>> execute_msg_avg = boltStats.get_execute_ms_avg();

                          /*  for (String key : execute_msg_avg.keySet()) {
                                System.out.println("Key: " + key);
                                System.out.println("What's in the execute_msg_avg.keySet() map?");
                                for (GlobalStreamId inner_key : execute_msg_avg.get(key).keySet()) {
                                    System.out.println("The inner key stream ID : " + inner_key.get_streamId());
                                    System.out.println("The inner key component ID : " + inner_key.get_componentId());
                                    System.out.println("The inner value : " + execute_msg_avg.get(key).get(inner_key).toString());
                                }
                            }*/

                            Map<GlobalStreamId, Double> statValues = execute_msg_avg.get(ALL_TIME);
                            Map<GlobalStreamId, Double> statValues_10Mins = execute_msg_avg.get(TEN_MINS);

                            for (GlobalStreamId key : statValues.keySet()) {
                                if (DEFAULT.equals(key.get_streamId())) {
                                    if (!temporaryExecuteLatency.containsKey(componentId)) {
                                        temporaryExecuteLatency.put(componentId, 0.0);
                                    }
                                    temporaryExecuteLatency.put(componentId, temporaryExecuteLatency.get(componentId) +
                                            statValues.get(key).doubleValue());
                              //      System.out.println("In temporaryExecuteLatency.get(" + componentId + "):  " + temporaryExecuteLatency.get(componentId));
                                }
                            }



                            for (GlobalStreamId key : statValues_10Mins.keySet()) {
                                if (DEFAULT.equals(key.get_streamId())) {
                                    if (!temporaryExecuteLatency_10Mins.containsKey(componentId)) {
                                        temporaryExecuteLatency_10Mins.put(componentId, 0.0);
                                    }
                                    temporaryExecuteLatency_10Mins.put(componentId, temporaryExecuteLatency_10Mins.get(componentId) +
                                            statValues_10Mins.get(key).doubleValue());
                                    //      System.out.println("In temporaryExecuteLatency.get(" + componentId + "):  " + temporaryExecuteLatency.get(componentId));
                                }
                            }
                        }

                        if (boltStats.is_set_process_ms_avg()) {
                            Map<String, Map<GlobalStreamId, Double>> process_msg_avg = boltStats.get_process_ms_avg();
                          /*  for (String key : process_msg_avg.keySet()) {
                                System.out.println("Key: " + key);
                                System.out.println("What's in the process_msg_avg.keySet() map?");
                                for (GlobalStreamId inner_key : process_msg_avg.get(key).keySet()) {
                                    System.out.println("The inner key stream ID : " + inner_key.get_streamId());
                                    System.out.println("The inner key component ID : " + inner_key.get_componentId());
                                    System.out.println("The inner value : " + process_msg_avg.get(key).get(inner_key).toString());
                                }
                            } */
                            // been through the loop
                            // component.setCompleteLatency(component.getCompleteLatency() / complete_msg_avg.keySet().size()); // COME BACK TO THIS LATER

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
                        //    System.out.println("In temporaryProcessLatency.get(" + componentId + "):  " + temporaryProcessLatency.get(componentId));
                        }
                        /*(defn compute-executor-capacity
                          [^ExecutorSummary e]
                          (let [stats (.get_stats e)
                                stats (if stats
                                        (-> stats
                                            (aggregate-bolt-stats true)
                                            (aggregate-bolt-streams)
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
                        // CALCULATING CAPACITY
                        //CRAP --> Gotta multiple executed tuples with latency and then divide with uptime. WT... :/ Why is this a problem? Executed tuples are hanging out in SLO.component -_-
                    }

                    if (temporaryExecuteLatency_10Mins.containsKey(componentId)) {
                        component.setExecuteLatency10mins(temporaryExecuteLatency_10Mins.get(componentId) / (double) component.getParallelism());
                        // CALCULATING CAPACITY
                        //CRAP --> Gotta multiple executed tuples with latency and then divide with uptime. WT... :/ Why is this a problem? Executed tuples are hanging out in SLO.component -_-
                    }

                    if (temporaryCompleteLatency.containsKey(componentId))
                        component.setCompleteLatency(temporaryCompleteLatency.get(componentId)  / (double) component.getParallelism());

                    writeToFile(latency_log, topologySchedule.getId() + "," + componentId + "," + component.getCompleteLatency() + "," + component.getExecuteLatency() + "," + component.getProcessLatency() + "," + System.currentTimeMillis() + "\n");
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