package backtype.storm.scheduler.advancedstela.slo;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.*;


public class Observer {
    private static final Logger LOG = LoggerFactory.getLogger(Observer.class);
    private static final String ALL_TIME = ":all-time";
    private static final String METRICS = "__metrics";
    private static final String SYSTEM = "__system";
    private static final String DEFAULT = "default";
    private static final String FAILED = "failed";
    private static final String ACKED = "acked";


    private Map config;
    private Topologies topologies;
    private NimbusClient nimbusClient;
    private File juice_log;
    private File flatline_log, outlier_log;


    public Observer(Map conf) {
        config = conf;
        topologies = new Topologies(config);
        juice_log = new File("/tmp/output.log");
        outlier_log = new File("/tmp/outlier.log");
        flatline_log = new File("/tmp/flat_line.log");
    }

    public TopologyPairs getTopologiesToBeRescaled() {
        return topologies.getTopologyPairScaling();
    }

    public void run() {
       // LOG.info("Running observer at: " + System.currentTimeMillis() / 1000);
        writeToFile(juice_log, "Running observer at: " + System.currentTimeMillis() + "\n");

        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));

                topologies.constructTopologyGraphs();
                HashMap<String, Topology> allTopologies = topologies.getStelaTopologies();
                writeToFile(flatline_log, "********* CLUSTER INFO: **********\n" + "NUMBER OF TOPOLOGIES: " + nimbusClient.getClient().getClusterInfo().get_topologies().size() + "\n"
                        + "NUMBER OF SUPERVISORS: " + nimbusClient.getClient().getClusterInfo().get_supervisors_size() + "\n"
                        + "NIMBUS UPTIME: " + nimbusClient.getClient().getClusterInfo().get_nimbus_uptime_secs() + "\n");
                Iterator <SupervisorSummary> supervisorSummaryIterator = nimbusClient.getClient().getClusterInfo().get_supervisors_iterator();
                while (supervisorSummaryIterator.hasNext())
                {
                    SupervisorSummary ss = supervisorSummaryIterator.next();
                    writeToFile(flatline_log, "SUPERVISOR ID: " + ss.get_supervisor_id() +"\n"
                            + "SUPERVISOR HOST: " + ss.get_host() + "\n"
                            + "SUPERVISOR USED WORKERS" + ss.get_num_used_workers() + "\n"
                            + "SUPERVISOR GET NUMBER OF TOTAL WORKERS" + ss.get_num_workers() + "\n"
                            + "SUPERVISOR GET UPTIME SECS" + ss.get_uptime_secs() + "\n");
                }


                cleanUpSLOMap(allTopologies); // To remove old spout values :D

                collectStatistics(allTopologies);
                calculateJuicePerSource(allTopologies);
                logFinalSourceJuicesPer(allTopologies);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void cleanUpSLOMap(HashMap<String, Topology> allTopologies) {
        // TODO Auto-generated method stub
        for (String topologyId : allTopologies.keySet()) {
            Topology topology = allTopologies.get(topologyId);
            HashMap<String, Component> spouts = topology.getSpouts();
            for (Component spout : spouts.values()) {
                spout.resetSpoutTransfer();
            }
            HashMap<String, Component> bolts = topology.getBolts();
            for (Component bolt : bolts.values()) {
                bolt.resetSpoutTransfer();
            }
        }
    }

    private void collectStatistics(HashMap<String, Topology> allTopologies) {
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
            HashMap<String, Integer> temporaryTransferred = new HashMap<>();
            HashMap<String, HashMap<String, Integer>> temporaryExecuted = new HashMap<>();
            HashMap<String, Long> temporaryFailed = new HashMap<>();
            HashMap<String, Long> temporaryAcked = new HashMap<>();
            for (ExecutorSummary executor : executorSummaries) {
                String componentId = executor.get_component_id();
                Component component = topology.getAllComponents().get(componentId);
                if (component == null) {
                    continue;
                }
                ExecutorStats stats = executor.get_stats();
                if (stats == null) {
                    continue;
                }
                ExecutorSpecificStats specific = stats.get_specific();
                Map<String, Map<String, Long>> transferred = stats.get_transferred();
                if (specific.is_set_spout()) {
                    // GETTING SPOUT TRANSFERRED
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (DEFAULT.equals(key)) {
                            if (!temporaryTransferred.containsKey(componentId)) {
                                temporaryTransferred.put(componentId, 0);
                            }
                            temporaryTransferred.put(componentId, temporaryTransferred.get(componentId) +
                                    statValues.get(key).intValue()); // WHY IS SHARANYA ADDING? -->Qs // BECAUSE THEY'RE DIFFERENT EXECUTORS AND WE NEED TO ADD THEM UP
                        }
                    }
                    // GETTING SPOUT ACKED + FAILED --- > the loop will run again and we'll get more values.
                    SpoutStats spout = specific.get_spout();
                    if (spout.is_set_acked()) {
                        /*DEBUG PRINTING*/
                       /* System.out.println("WHAT TOPOLOGY IS THIS? :" + topologyId);
                        spout.get_acked().get(ALL_TIME);

                        System.out.println("In spout.is_set_acked() ");
                        Map<String, Map<String, Long>> acked = spout.get_acked();
                        System.out.println("Printing Keys: for acked spouts");
                        for (String key : acked.keySet()) {
                            System.out.println("Key: " + key);
                            System.out.println("What's in the map?");
                            for (String inner_key : acked.get(key).keySet()) {
                                System.out.println("The inner key : " + inner_key);
                                System.out.println("The inner value : " + acked.get(key).get(inner_key).toString());
                            }
                        } *////

                      //  System.out.println("Hopefully this is the spout ID:  " + executor.get_component_id());

                        if (spout.get_acked().get(ALL_TIME).containsKey(DEFAULT)) {
                            if (!temporaryAcked.containsKey(componentId)) {
                                temporaryAcked.put(componentId, 0L);
                            }

                           // System.out.println("DEBUGGING THE NULLPOINTEREXCEPTION: " + spout.get_acked().get(ALL_TIME).get(DEFAULT));
                            temporaryAcked.put(componentId, temporaryAcked.get(componentId) +
                                    spout.get_acked().get(ALL_TIME).get(DEFAULT));
                           // System.out.println("Value put in the acked map:  " + temporaryAcked.get(componentId));
                        }
                    }
                    if (spout.is_set_failed()) {
                        /*System.out.println("In spout.is_set_failed() ");
                        Map<String, Map<String, Long>> failed = spout.get_failed();
                        System.out.println("Printing Keys: for failed spouts");
                        for (String key : failed.keySet()) {
                            System.out.println("Key: " + key);
                            System.out.println("What's in the map?");

                            for (String inner_key : failed.get(key).keySet()) {
                                System.out.println("The inner key : " + inner_key);
                                System.out.println("The inner value : " + failed.get(key).get(inner_key).toString());
                            }
                        }*/
                        //System.out.println("Hopefully this is the spout ID:  " + executor.get_component_id());

                        if (spout.get_failed().get(ALL_TIME).containsKey(DEFAULT)) {
                            if (!temporaryFailed.containsKey(componentId)) {
                                temporaryFailed.put(componentId, 0L);
                            }
                            temporaryFailed.put(componentId, temporaryFailed.get(componentId) +
                                    spout.get_failed().get(ALL_TIME).get(DEFAULT));
                         //   System.out.println("Value put in the failed map:  " + temporaryFailed.get(componentId));
                        }
                    }
                } else {
                    Map<String, Long> statValues = transferred.get(ALL_TIME);
                    for (String key : statValues.keySet()) {
                        if (DEFAULT.equals(key)) {
                            if (!temporaryTransferred.containsKey(componentId)) {
                                temporaryTransferred.put(componentId, 0);
                            }
                            temporaryTransferred.put(componentId, temporaryTransferred.get(componentId) +
                                    statValues.get(key).intValue());
                        }
                    }
                    Map<String, Map<GlobalStreamId, Long>> executed = specific.get_bolt().get_executed();
                    Map<GlobalStreamId, Long> executedStatValues = executed.get(ALL_TIME);
                    for (GlobalStreamId streamId : executedStatValues.keySet()) {
                        if (!temporaryExecuted.containsKey(componentId)) {
                            temporaryExecuted.put(componentId, new HashMap<String, Integer>());
                        }

                        if (!temporaryExecuted.get(componentId).containsKey(streamId.get_componentId())) {
                            temporaryExecuted.get(componentId).put(streamId.get_componentId(), 0);
                        }

                        temporaryExecuted.get(componentId).put(streamId.get_componentId(),
                                temporaryExecuted.get(componentId).get(streamId.get_componentId()) +
                                        executedStatValues.get(streamId).intValue());

                        writeToFile(outlier_log, topologyId + "," + componentId + "," + executedStatValues.get(streamId).intValue() + "\n");
                    }
                }
            }
            for (String componentId : topology.getAllComponents().keySet()) {
                Component component = topology.getAllComponents().get(componentId);
                if (temporaryTransferred.containsKey(componentId)) {
                    component.setCurrentTransferred(temporaryTransferred.get(componentId));
                    component.setTotalTransferred(temporaryTransferred.get(componentId));

                }
                if (temporaryExecuted.containsKey(componentId)) {
                    HashMap<String, Integer> executedValues = temporaryExecuted.get(componentId);
                    for (String source : executedValues.keySet()) {
                        component.addCurrentExecuted(source, executedValues.get(source));
                        component.addTotalExecuted(source, executedValues.get(source));
                    }
                }
// SO THIS SHOULD ALREADY BE A SPOUT
                if (temporaryAcked.containsKey(componentId)) { // first iteration could be a problem but it won't be because every
                   // System.out.println("KEY for getting final acked: "+ componentId);

                    component.setCurrentAcked(temporaryAcked.get(componentId));
                    component.setTotalAcked(temporaryAcked.get(componentId));

                    //System.out.println("component current acked: " + component.getCurrentAcked());
                   // System.out.println("component total acked: "+ component.getTotalAcked());
                }

                if (temporaryFailed.containsKey(componentId)) {
                  //  System.out.println("KEY for getting final failed: "+ componentId);
                    component.setCurrentFailed(temporaryFailed.get(componentId));
                    component.setTotalFailed(temporaryFailed.get(componentId));

                  //  System.out.println("component current failed: " + component.getCurrentFailed());
                  //  System.out.println("component total failed: " + component.getTotalFailed());
                }
            }
        }
    }

    private void calculateJuicePerSource(HashMap<String, Topology> allTopologies) {

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
                    writeToFile(outlier_log, topologyId + "," + spout.getId() + "," + currentTransferred + "," + executed + "," + value + "\n");

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

                            writeToFile(outlier_log, topologyId + "," + bolt.getId() + "," + currentTransferred + "," + executed + "," + value + "\n");

                        }
                        children.put(stelaComponent.getId(), stelaComponent);
                    }
                }

                parents = children;
            }
        }

    }

    private void logFinalSourceJuicesPer(HashMap<String, Topology> allTopologies) {


        for (String topologyId : allTopologies.keySet()) {
            Double calculatedSLO = 0.0;
            Topology topology = allTopologies.get(topologyId);

            int spouts_transferred = 0;
            int sink_executed = 0;
            Long all_acked = 0L;
            Long all_failed = 0L;
            Long total = 0L;
            for (Map.Entry <String, Component> spout : topology.getSpouts().entrySet())
            {

                spouts_transferred += spout.getValue().getCurrentTransferred();
                all_acked += spout.getValue().getCurrentAcked(); // SHOULD WE GET PREV OR CURR --> ASK
                all_failed += spout.getValue().getCurrentFailed();


              //  System.out.println("Curr Acked: " + spout.getValue().getCurrentAcked());
              //  System.out.println("Curr Failed: " + spout.getValue().getCurrentFailed());
            }
            total = all_acked + all_failed;

            for (Component bolt : topology.getBolts().values()) {
                if (bolt.getChildren().isEmpty()) {
                    for (Double sourceProportion : bolt.getSpoutTransfer().values()) {
                        calculatedSLO += sourceProportion;
                    }
                    for (Integer boltExecuted : bolt.getCurrentExecuted().values()) {
                        sink_executed += boltExecuted;
                    }
                }
            }

            calculatedSLO = calculatedSLO / topology.getSpouts().size(); // DO NOT ACCOUNT FOR FAILURES
        //    System.out.println("Topology name: " +topologyId);
        //    System.out.println("Before the failure calc - SLO: " + calculatedSLO);
        //    System.out.println("Failed: " + all_failed);
        //    System.out.println("Acked: " + all_acked);
        //    System.out.println("Total: " + total);
            if (all_failed > 0){ // we've got to account for replays
        //        System.out.println("We saw failures. WOO");
                Double factor_of_multiplication = (double) all_acked / (double) total;
        //        System.out.println("Factor of multiplication: " + factor_of_multiplication);
                calculatedSLO *= factor_of_multiplication; // MULTIPLY SLO BY acked
            }
        //    System.out.println("After the failure calc ");
        //    System.out.println("SLO after failure calc : " + calculatedSLO);

            topology.setMeasuredSLOs(calculatedSLO);
            writeToFile(juice_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + spouts_transferred + "," + sink_executed + "," + System.currentTimeMillis() + "\n");
            writeToFile(outlier_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + System.currentTimeMillis() + "\n");
            writeToFile(flatline_log, topologyId + "," + calculatedSLO + "," + topology.getMeasuredSLO() + "," + System.currentTimeMillis() + "\n");
        }

    }

    public void clearTopologySLOs(String topologyId) {
        topologies.remove(topologyId);
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.append(data);
            bufferWritter.close();
            fileWritter.close();
        } catch (IOException ex) {
          System.out.println(ex.toString());
        }
    }

    public void updateLastRebalancedTime(String topologyId, Long time)
    {
        topologies.updateLastRebalancedTime(topologyId, time);
    }
}
