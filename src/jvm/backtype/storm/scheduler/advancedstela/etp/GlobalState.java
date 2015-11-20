package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.scheduler.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalState {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalState.class);

    private Map config;
    private NimbusClient nimbusClient;

    /* Topology schedules which store the schedule state of the topology. */
    private HashMap<String, TopologySchedule> topologySchedules;

    /* Supervisor to node mapping. */
    private HashMap<String, Node> supervisorToNode;

    public GlobalState(Map conf) {
        config = conf;
        topologySchedules = new HashMap<String, TopologySchedule>();
        supervisorToNode = new HashMap<String, Node>();

        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST),
                        (Integer) config.get(Config.NIMBUS_THRIFT_PORT));
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }

    public HashMap<String, TopologySchedule> getTopologySchedules() {
        return topologySchedules;
    }

    public HashMap<String, Node> getSupervisorToNode() {
        return supervisorToNode;
    }

    public void collect(Cluster cluster, Topologies topologies) {
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

                addSpoutsAndBolts(topology, topologySchedule);
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
            for (TopologyDetails topologyDetails : topologies.getTopologies()) {
                TopologyInfo topologyInformation = nimbusClient.getClient().getTopologyInfo(topologyDetails.getId());
                TopologySchedule topologySchedule = topologySchedules.get(topologyDetails.getId());
                for (Map.Entry<ExecutorDetails, String> executorToComponent :
                        topologyDetails.getExecutorToComponent().entrySet()) {
                    Component component = topologySchedule.getComponents().get(executorToComponent.getValue());
                    component.addExecutor(executorToComponent.getKey());
                    topologySchedule.addExecutorToComponent(executorToComponent.getKey(), component.getId());
                }

                for (ExecutorSummary executorSummary: topologyInformation.get_executors()) {
                    Component component = topologySchedule.getComponents().get(executorSummary.get_component_id());
                    component.addExecutorSummary(executorSummary);
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

    private void addSpoutsAndBolts(StormTopology stormTopology, TopologySchedule topologySchedule) throws TException {
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
}
