package backtype.storm.scheduler.advancedstela;

import backtype.storm.scheduler.*;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.advancedstela.etp.*;
import backtype.storm.scheduler.advancedstela.slo.Observer;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import backtype.storm.scheduler.advancedstela.slo.TopologyPairs;
import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.*;

public class AdvancedStelaScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AdvancedStelaScheduler.class);
    private static final Integer OBSERVER_RUN_INTERVAL = 30;

    @SuppressWarnings("rawtypes")
    private Map config;
    private Observer sloObserver;
    private GlobalState globalState;
    private GlobalStatistics globalStatistics;
    private OperatorSelector selector;
    private HashMap<String, ExecutorPair> targets, victims;
    private File juice_log;
    private ArrayList <History> history;

    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        juice_log = new File("/tmp/output.log");
        config = conf;
        sloObserver = new Observer(conf);
        globalState = new GlobalState(conf);
        globalStatistics = new GlobalStatistics(conf);
        selector = new OperatorSelector();
        victims = new HashMap<String, ExecutorPair>();
        targets = new HashMap<String, ExecutorPair>();
        history = new ArrayList<>();
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        logUnassignedExecutors(cluster.needsSchedulingTopologies(topologies), cluster);
        int numTopologiesThatNeedScheduling = cluster.needsSchedulingTopologies(topologies).size();
        for (String target : targets.keySet()) {
            LOG.info("target {} ", target);
        }
        for (String victim : victims.keySet()) {
            LOG.info("victim {} ", victim);
        }

        runAdvancedStelaComponents(cluster, topologies);
        if (numTopologiesThatNeedScheduling > 0) {
            LOG.info("STORM IS GOING TO PERFORM THE REBALANCING");
            new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
        } else if (numTopologiesThatNeedScheduling == 0) {
            LOG.info("((victims.isEmpty() && targets.isEmpty()) && numTopologiesThatNeedScheduling == 0 && numTopologies > 0)");
            LOG.info("Printing topology schedules");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            TopologyPairs topologiesToBeRescaled = sloObserver.getTopologiesToBeRescaled();

            ArrayList<String> receivers = topologiesToBeRescaled.getReceivers();
            ArrayList<String> givers = topologiesToBeRescaled.getGivers();

            ArrayList<Topology> receiver_topologies = new ArrayList<Topology>();
            ArrayList<Topology> giver_topologies = new ArrayList<Topology>();

            for (int i = 0; i < receivers.size(); i++)
                receiver_topologies.add(sloObserver.getTopologyById(receivers.get(i)));

            for (int i = 0; i < givers.size(); i++)
                giver_topologies.add(sloObserver.getTopologyById(givers.get(i)));

            LOG.info("Length of givers {}", giver_topologies.size());
            LOG.info("Length of receivers {}", receiver_topologies.size());

            if (receiver_topologies.size() > 0 && giver_topologies.size() > 0) {

                ArrayList<String> topologyPair = new TopologyPicker().classBasedStrategy(receiver_topologies, giver_topologies);//unifiedStrategy(receiver_topologies, giver_topologies);//.worstTargetBestVictim(receivers, givers);
                String receiver = topologyPair.get(0);

                LOG.info("Picked the first two topologies for rebalance");

                TopologyDetails target = topologies.getById(receiver);
                TopologySchedule targetSchedule = globalState.getTopologySchedules().get(receiver);
                Component targetComponent = selector.selectOperator(globalState, globalStatistics, sloObserver.getTopologyById(receiver));
                LOG.info("target before rebalanceTwoTopologies {} ", target.getId());

                if (targetComponent != null) {
                    LOG.info("topology {} target component", receiver, targetComponent.getId());
                    rebalanceTopology(target, targetSchedule, targetComponent, topologies);
                }
            } else if (giver_topologies.size() == 0) {
                StringBuffer sb = new StringBuffer();
                LOG.info("There are no givers! *Sob* \n");
                LOG.info("Receivers:  \n");

                for (int i = 0; i < receiver_topologies.size(); i++)
                    LOG.info(receiver_topologies.get(i).getId() + "\n");

            } else if (receiver_topologies.size() == 0) {
                StringBuffer sb = new StringBuffer();
                LOG.info("There are no receivers! *Sob* \n");
                LOG.info("Givers:  \n");

                for (int i = 0; i < giver_topologies.size(); i++)
                    LOG.info(giver_topologies.get(i).getId() + "\n");

            }
        }
    }

    private void rebalanceTopology(TopologyDetails targetDetails,
                                        TopologySchedule target,
                                        Component component,
                                   Topologies topologies) {
        LOG.info("In rebalance topology");
        if (config != null) {
            try {
                int one = 1;
                String targetComponent = component.getId();
                Integer targetOldParallelism = target.getComponents().get(targetComponent).getParallelism();
                Integer targetNewParallelism = targetOldParallelism + one;
                String targetCommand = "/var/nimbus/storm/bin/storm " +
                        "rebalance " + targetDetails.getName() + " -w 0 -e " +
                        targetComponent + "=" + targetNewParallelism;
                target.getComponents().get(targetComponent).setParallelism(targetNewParallelism);
                try {
                    writeToFile(juice_log, targetCommand + "\n");
                    writeToFile(juice_log, System.currentTimeMillis() + "\n");
                    LOG.info(targetCommand + "\n");
                    LOG.info(System.currentTimeMillis() + "\n");
                    LOG.info("running the rebalance using storm's rebalance command \n");
                    LOG.info("Target old executors count {}", targetDetails.getExecutors().size());

                    saveHistory (topologies);

                    Runtime.getRuntime().exec(targetCommand);
                    sloObserver.updateLastRebalancedTime(target.getId(), System.currentTimeMillis() / 1000);
                    sloObserver.clearTopologySLOs(target.getId());
                } catch (Exception e) {
                    LOG.info(e.toString());
                    LOG.info("In first exception");
                    //e.printStackTrace();
                }
            } catch (Exception e) {
                LOG.info(e.toString());
                LOG.info("In second exception");
                //e.printStackTrace();
                return;
            }
        }
    }

    private void runAdvancedStelaComponents(Cluster cluster, Topologies topologies) {
        sloObserver.run();
        globalState.collect(cluster, topologies);
        globalState.setCapacities(sloObserver.getAllTopologies());
        globalStatistics.collect();
    }

    private void logUnassignedExecutors(List<TopologyDetails> topologiesScheduled, Cluster cluster) {
        for (TopologyDetails topologyDetails : topologiesScheduled) {
            Collection<ExecutorDetails> unassignedExecutors = cluster.getUnassignedExecutors(topologyDetails);

            if (unassignedExecutors.size() > 0) {
                for (ExecutorDetails executorDetails : unassignedExecutors) {
                    LOG.info("executorDetails.toString(): " + executorDetails.toString() + "\n");
                }
            }
        }
    }


   public void saveHistory(Topologies topologies) {
       LOG.info("save History");
       HashMap <String, TopologyDetails> tops = new HashMap<> ();

       for (TopologyDetails t : topologies.getTopologies()) {
           tops.put(t.getId(), t);
       }
       History h = new History(tops, sloObserver.getAllTopologies());
       history.add(h);

       HashMap<String, Topology> topologiesPerformance = h.getTopologiesPerformance();
       HashMap<String, TopologyDetails> topologySchedules = h.getTopologiesSchedule();

       for (Map.Entry<String, Topology> entry : topologiesPerformance.entrySet()) {
           Topology t = entry.getValue();
           LOG.info(" name {} topology {} average latency {} latency slo {} juice {} throughput slo {} ", entry.getKey(), t.getId(), t.getAverageLatency(), t.getUserSpecifiedLatencySLO(), t.getUserSpecifiedSLO(), t.getMeasuredSLO());
       }

       for (Map.Entry<String, TopologyDetails> entry : topologySchedules.entrySet()) {
           TopologyDetails t = entry.getValue();
           LOG.info(" name {} topology {} workers {} conf {} num of executors {}" +
                           " executors {} ",
                   entry.getKey(),
                   t.getId(),
                   t.getNumWorkers(),
                   t.getConf(),
                   t.getExecutorToComponent().size(),
                   t.getExecutors());
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