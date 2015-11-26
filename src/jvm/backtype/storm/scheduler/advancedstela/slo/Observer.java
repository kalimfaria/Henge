package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class Observer {
    private static final Logger LOG = LoggerFactory.getLogger(Observer.class);
    private static final String ALL_TIME = ":all-time";
    private static final String METRICS = "__metrics";
    private static final String SYSTEM = "__system";
    private static final String DEFAULT = "default";

    private Map config;
    private Topologies topologies;
    private NimbusClient nimbusClient;

    public Observer(Map conf) {
        config = conf;
        topologies = new Topologies(config);
    }

    public TopologyPairs getTopologiesToBeRescaled() {
        return topologies.getTopologyPairScaling();
    }

    public void run() {
        LOG.info("********************* OBSERVER LOGGING START *******************************");
        LOG.info("Running observer at: " + System.currentTimeMillis() / 1000);

        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));
                topologies.constructTopologyGraphs();
                HashMap<String, Topology> allTopologies = topologies.getStelaTopologies();

                collectStatistics(allTopologies);
                calculateSloPerSource(allTopologies);
                logFinalSourceSLOsPer(allTopologies);
                LOG.info("************************* OBSERVER LOGGING END *******************************");

            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }

    private void collectStatistics(HashMap<String, Topology> allTopologies) {
        LOG.info("************************* Collect Statistics Start *******************************");
        for (String topologyId : allTopologies.keySet()) {
            Topology topology = allTopologies.get(topologyId);
            TopologyInfo topologyInfo = null;

            try {
                topologyInfo = nimbusClient.getClient().getTopologyInfo(topologyId);
            } catch (TException e) {
                LOG.error(e.toString());
                continue;
            }

            List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();

            for (ExecutorSummary executor : executorSummaries) {
                String componentId = executor.get_component_id();
                Component component = topology.getAllComponents().get(componentId);

                if (component == null) {
                    continue;
                }

                LOG.info("\nComponent: {}", component.getId());
                ExecutorStats stats = executor.get_stats();

                if (stats == null) {
                    continue;
                }

                ExecutorSpecificStats specific = stats.get_specific();
                Map<String, Map<String, Long>> transferred = stats.get_transferred();

                if (specific.is_set_spout()) {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (DEFAULT.equals(key)) {
                            LOG.info("Key = {}, Value = {}", key, statValues.get(key));
                            LOG.info("Total Transferred: {}", component.getTotalTransferred());
                            LOG.info("Current Transferred: {}", statValues.get(key).intValue());
                            component.setCurrentTransferred(statValues.get(key).intValue());
                            component.setTotalTransferred(statValues.get(key).intValue());

                            LOG.info("Spout value found: {}", statValues.get(key).intValue());
                            LOG.info("Spout Current Transferred: {}, Total Transferred: {}",
                                    component.getCurrentTransferred(), component.getTotalTransferred());
                        }
                    }
                } else {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (DEFAULT.equals(key)) {
                            component.setCurrentTransferred(statValues.get(key).intValue());
                            component.setTotalTransferred(statValues.get(key).intValue());
                            LOG.info("Key = {}, Value = {}", key, statValues.get(key));
                            LOG.info("Bolt value found: {}", statValues.get(key).intValue());
                            LOG.info("Bolt Current Transferred: {}, Total Transferred: {}",
                                    component.getCurrentTransferred(), component.getTotalTransferred());
                        }
                    }

                    Map<String, Map<GlobalStreamId, Long>> executed = specific.get_bolt().get_executed();
                    Map<GlobalStreamId, Long> executedStatValues = executed.get(ALL_TIME);
                    for (GlobalStreamId streamId : executedStatValues.keySet()) {
                        component.addCurrentExecuted(streamId.get_componentId(),
                                executedStatValues.get(streamId).intValue());
                        component.addTotalExecuted(streamId.get_componentId(),
                                executedStatValues.get(streamId).intValue());
                        LOG.info("Key = {}, Value = {}", streamId.get_componentId(), executedStatValues.get(streamId).intValue());
                        LOG.info("Bolt value found: {}", executedStatValues.get(streamId).intValue());
                        LOG.info("Bolt Current Executed: Parent {}, Current Executed {}, Total Executed {}",
                                streamId.get_componentId(),
                                component.getCurrentExecuted().get(streamId.get_componentId()),
                                component.getTotalExecuted().get(streamId.get_componentId()));
                    }
                }
            }
        }
        LOG.info("************************* Collect Statistics End *******************************");
    }

    private void calculateSloPerSource(HashMap<String, Topology> allTopologies) {
        for (String topologyId : allTopologies.keySet()) {
            Topology topology = allTopologies.get(topologyId);
            HashMap<String, Component> spouts = topology.getSpouts();

            HashMap<String, Component> parents = new HashMap<String, Component>();
            for (Component spout : spouts.values()) {
                HashSet<String> children = spout.getChildren();
                for (String child : children) {
                    Component component = topology.getAllComponents().get(child);

                    Integer currentTransferred = spout.getCurrentTransferred();
                    Integer executed = component.getCurrentExecuted().get(spout.getId());

                    if (executed == null || currentTransferred == null) {
                        continue;
                    }

                    Double value;
                    if (currentTransferred == 0) {
                        value = 1.0;
                    } else {
                        value = ((double) executed) / (double) currentTransferred;
                    }

                    component.addSpoutTransfer(spout.getId(), value);
                    parents.put(child, component);
                }
            }

            while (!parents.isEmpty()) {
                HashMap<String, Component> children = new HashMap<String, Component>();
                for (Component bolt : parents.values()) {
                    HashSet<String> boltChildren = bolt.getChildren();

                    for (String child : boltChildren) {
                        Component stelaComponent = topology.getAllComponents().get(child);

                        Integer currentTransferred = bolt.getCurrentTransferred();
                        Integer executed = stelaComponent.getCurrentExecuted().get(bolt.getId());

                        if (executed == null || currentTransferred == null) {
                            continue;
                        }

                        Double value;
                        if (currentTransferred == 0) {
                            value = 1.0;
                        } else {
                            value = ((double) executed) / (double) currentTransferred;
                        }

                        for (String source : bolt.getSpoutTransfer().keySet()) {
                            stelaComponent.addSpoutTransfer(source,
                                    value * bolt.getSpoutTransfer().get(source));
                        }
                        children.put(stelaComponent.getId(), stelaComponent);
                    }
                }

                parents = children;
            }
        }
    }

    private void logFinalSourceSLOsPer(HashMap<String, Topology> allTopologies) {
        LOG.info("**************************************************************************************************");

        for (String topologyId : allTopologies.keySet()) {
            Double calculatedSLO = 0.0;
            Topology topology = allTopologies.get(topologyId);

            LOG.info("User specified SLO for topology {} is {}", topologyId, topology.getUserSpecifiedSLO());

            for (Component bolt : topology.getBolts().values()) {
                if (bolt.getChildren().isEmpty()) {
                    for (Double sourceProportion : bolt.getSpoutTransfer().values()) {
                        LOG.info("{}", sourceProportion);
                        calculatedSLO += sourceProportion;
                    }
                }
            }


            LOG.info("Total spouts number is {}", topology.getSpouts().size());
            calculatedSLO = calculatedSLO / topology.getSpouts().size();
            topology.setMeasuredSLOs(calculatedSLO);
            LOG.info("SLO values are [{}].", topology.printSLOs());
            LOG.info("Measured SLO for topology {} for this run is {} and average slo is {}.", topologyId, calculatedSLO,
                    topology.getMeasuredSLO());
            LOG.info("**************************************************************************************************");
        }

    }
}
