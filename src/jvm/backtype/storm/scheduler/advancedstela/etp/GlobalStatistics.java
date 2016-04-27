package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalStatistics {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalStatistics.class);
    private static final Integer MOVING_AVG_WINDOW = 30;
    private static final String ALL_TIME = ":all-time";
    private static final String DEFAULT = "default";
    private static final String METRICS = "__metrics";
    private static final String SYSTEM = "__system";
    private static final String TRANSFER = "transfer";
    private static final String EMIT = "emit";

    private Map config;
    private NimbusClient nimbusClient;

    private HashMap<String, NodeStatistics> nodeStatistics;
    private HashMap<String, TopologyStatistics> topologyStatistics;
    public HashMap<String, Integer> transferStatsTable;
    public HashMap<String, Integer> emitStatsTable;
    public HashMap<String, Integer> emitRatesTable;
    public HashMap<String, Integer> executeStatsTable;
    public HashMap<String, Integer> executeRatesTable;

    public GlobalStatistics(Map conf) {
        config = conf;
        nodeStatistics = new HashMap<String, NodeStatistics>();
        topologyStatistics = new HashMap<String, TopologyStatistics>();
        transferStatsTable = new HashMap<String, Integer>();
        emitStatsTable = new HashMap<String, Integer>();
        emitRatesTable = new HashMap<String, Integer>();
        executeStatsTable = new HashMap<String, Integer>();
        executeRatesTable = new HashMap<String, Integer>();
    }

    public HashMap<String, TopologyStatistics> getTopologyStatistics() {
        return topologyStatistics;
    }

    public void collect() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }

        nodeStatistics.clear();
        for (TopologyStatistics statistics : topologyStatistics.values()) {
            statistics.clearComponentStatistics();
        }

        try {
            List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();
            for (TopologySummary topologySummary : topologies) {
                String id = topologySummary.get_id();
                TopologyStatistics statistics;

                if (topologyStatistics.containsKey(id)) {
                    statistics = topologyStatistics.get(id);
                } else {
                    statistics = new TopologyStatistics(id);
                    statistics.setStartupTime((System.currentTimeMillis() / 1000));
                    topologyStatistics.put(id, statistics);
                }

                TopologyInfo topologyInformation = null;
                StormTopology stormTopology = null;
                try {
                    topologyInformation = nimbusClient.getClient().getTopologyInfo(id);
                    stormTopology = nimbusClient.getClient().getTopology(id);

                } catch (Exception e) {
                    LOG.error(e.toString());
                    continue;
                }

                List<ExecutorSummary> executors = topologyInformation.get_executors();
                for (ExecutorSummary executorSummary : executors) {
                    String componentId = executorSummary.get_component_id();
                    if (!componentId.matches("(__).*")) {
                        populateNodeStatisticsForExecutor(executorSummary, stormTopology);
                        populateComponentStatistics(executorSummary, statistics);

                        ExecutorStats executorStats = executorSummary.get_stats();
                        if (executorStats == null) {
                            continue;
                        }

                        ExecutorSpecificStats execSpecStats = executorStats.get_specific();

                        BoltStats boltStats = null;
                        if (execSpecStats.is_set_bolt()) {
                            boltStats = execSpecStats.get_bolt();

                        }

                        Map<String, Long> transferred = executorStats.get_transferred().get(ALL_TIME);
                        Map<String, Long> emitted = executorStats.get_emitted().get(ALL_TIME);

                        if (transferred.get(DEFAULT) != null && emitted.get(DEFAULT) != null) {
                            String taskId = constructTaskHashID(executorSummary, topologySummary);
                            Integer totalTransferOutput = transferred.get(DEFAULT).intValue();
                            Integer totalEmitOutput = emitted.get(DEFAULT).intValue();
                            Integer totalExecuted = 0;

                            if (boltStats != null) {
                                totalExecuted = getTotalExecutedAtBolt(boltStats.get_executed(), ALL_TIME).intValue();

                                if (execSpecStats.is_set_bolt()) {
                                    if (!executeStatsTable.containsKey(taskId)) {
                                        executeStatsTable.put(taskId, totalExecuted);
                                    }

                                    if (!executeRatesTable.containsKey(taskId)) {
                                        executeRatesTable.put(taskId, 0);
                                    }
                                }
                            }

                            if (!transferStatsTable.containsKey(taskId)) {
                                transferStatsTable.put(taskId, totalTransferOutput);
                            }

                            if (!emitStatsTable.containsKey(taskId)) {
                                emitStatsTable.put(taskId, totalEmitOutput);
                                emitRatesTable.put(taskId, 0);
                            }

                            Integer transferThroughput = totalTransferOutput - transferStatsTable.get(taskId);
                            if (transferThroughput < 0) {
                                transferThroughput = 0;
                            }

                            Integer emitThroughput = totalEmitOutput - emitStatsTable.get(taskId);
                            if (emitThroughput < 0) {
                                emitThroughput = 0;
                            }

                            emitRatesTable.put(taskId, emitThroughput);

                            Integer executeThroughput = 0;
                            if (executeStatsTable.containsKey(taskId)) {
                                executeThroughput = totalExecuted - executeStatsTable.get(taskId);
                                executeRatesTable.put(taskId, executeThroughput);
                            }

                            transferStatsTable.put(taskId, totalTransferOutput);
                            emitStatsTable.put(taskId, totalEmitOutput);
                            executeStatsTable.put(taskId, totalExecuted);

                            String host = executorSummary.get_host();
                            nodeStatistics.get(host).addTransferThroughput(transferThroughput);
                            nodeStatistics.get(host).addEmitThroughput(emitThroughput);

                            if (stormTopology.get_bolts().containsKey(componentId)) {
                                nodeStatistics.get(host).throughputForBolts.put(TRANSFER,
                                        nodeStatistics.get(host).throughputForBolts.get(TRANSFER)
                                                + transferThroughput);

                                nodeStatistics.get(host).throughputForBolts.put(EMIT,
                                        nodeStatistics.get(host).throughputForBolts.get(EMIT)
                                                + emitThroughput);
                            } else if (stormTopology.get_bolts().containsKey(componentId)) {
                                nodeStatistics.get(host).throughputForSpouts.put(TRANSFER,
                                        nodeStatistics.get(host).throughputForSpouts.get(TRANSFER)
                                                + transferThroughput);

                                nodeStatistics.get(host).throughputForSpouts.put(EMIT,
                                        nodeStatistics.get(host).throughputForSpouts.get(EMIT)
                                                + emitThroughput);
                            }

                            ComponentStatistics componentStats = statistics.getComponentStatistics().get(componentId);

                            if (componentStats != null) {
                                componentStats.totalTransferThroughput += transferThroughput;
                                componentStats.totalEmitThroughput += emitThroughput;
                                componentStats.totalExecuteThroughput += executeThroughput;
                            }
                        }
                    }
                }

                if(executors.size()>0) {
                    updateThroughputHistory(topologySummary);
                }
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void updateThroughputHistory(TopologySummary topologySummary) {
        TopologyStatistics statistics = topologyStatistics.get(topologySummary.get_id());
        if (statistics != null) {
            HashMap<String, List<Integer>> componentTransferHistory = statistics.getTransferThroughputHistory();
            HashMap<String, List<Integer>> componentEmitHistory = statistics.getEmitThroughputHistory();
            HashMap<String, List<Integer>> componentExecuteHistory = statistics.getExecuteThroughputHistory();

            for (Map.Entry<String, ComponentStatistics> entry : statistics.getComponentStatistics().entrySet()) {
                if (!componentTransferHistory.containsKey(entry.getKey())) {
                    componentTransferHistory.put(entry.getKey(), new ArrayList<Integer>());
                }

                if (!componentEmitHistory.containsKey(entry.getKey())) {
                    componentEmitHistory.put(entry.getKey(), new ArrayList<Integer>());
                }

                if (!componentExecuteHistory.containsKey(entry.getKey())) {
                    componentExecuteHistory.put(entry.getKey(), new ArrayList<Integer>());
                }

                if (componentTransferHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
                    componentTransferHistory.get(entry.getKey()).remove(0);
                }

                if (componentEmitHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
                    componentEmitHistory.get(entry.getKey()).remove(0);
                }

                if (componentExecuteHistory.get(entry.getKey()).size() >= MOVING_AVG_WINDOW) {
                    componentExecuteHistory.get(entry.getKey()).remove(0);
                }

                componentTransferHistory.get(entry.getKey()).add(entry.getValue().totalTransferThroughput);
                componentEmitHistory.get(entry.getKey()).add(entry.getValue().totalEmitThroughput);
                componentExecuteHistory.get(entry.getKey()).add(entry.getValue().totalExecuteThroughput);
            }
        }
    }

    private String constructTaskHashID(ExecutorSummary executorSummary, TopologySummary topologySummary) {
        return executorSummary.get_host() + ':'
                + executorSummary.get_port() + ':'
                + executorSummary.get_component_id() + ":"
                + topologySummary.get_id() + ":"
                + Integer.toString(executorSummary.get_executor_info().get_task_start());
    }

    private void populateNodeStatisticsForExecutor(ExecutorSummary executorSummary, StormTopology stormTopology) {
        String host = executorSummary.get_host();

        if (!nodeStatistics.containsKey(host)) {
            nodeStatistics.put(host, new NodeStatistics(host));
        }

        NodeStatistics statisticsForNode = this.nodeStatistics.get(host);
        if (stormTopology.get_bolts().containsKey(executorSummary.get_component_id())) {
            statisticsForNode.addBolt(executorSummary);
        } else if (stormTopology.get_spouts().containsKey(host)) {
            statisticsForNode.addSpout(executorSummary);
        }
    }

    private void populateComponentStatistics(ExecutorSummary executorSummary, TopologyStatistics statistics) {
        String componentId = executorSummary.get_component_id();
        if (!statistics.getComponentStatistics().containsKey(componentId)) {
            statistics.addComponentStatistics(componentId, new ComponentStatistics(componentId));
        }
    }

    private static Long getTotalExecutedAtBolt(Map<String, Map<GlobalStreamId, Long>> map, String statName) {
        Long value = (long) 0;
        Map<GlobalStreamId, Long> intermediateMap = map.get(statName);
        for (Long val : intermediateMap.values()) {
            value += val;
        }
        return value;
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
        } catch (IOException ex) {
            System.out.println(ex.toString());
        }
    }
}
