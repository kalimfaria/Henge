package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Topologies {
    private static final Integer UP_TIME = 10*60;
    private static final Integer REBALANCING_INTERVAL = 60*5;

    private Map config;
    private NimbusClient nimbusClient;
    private HashMap<String, Topology> stelaTopologies;
    private HashMap<String, Long> topologiesUptime;
    private HashMap<String, Long> lastRebalancedAt;
    private File flatline_log;

    public Topologies(Map conf) {
        config = conf;
        stelaTopologies = new HashMap<String, Topology>();
        topologiesUptime = new HashMap<String, Long>();
        lastRebalancedAt = new HashMap<String, Long>();
        flatline_log = new File("/tmp/flat_line.log");
    }

    public HashMap<String, Topology> getStelaTopologies() {
        return stelaTopologies;
    }

    public TopologyPairs getTopologyPairScaling() { // when trying to add topologies to either of these

        // when clearing topology SLO, mark the time
        // when adding topologies back, I can check if that old time is greater than that time + the amount I want to stagger it for
        ArrayList<Topology> failingTopologies = new ArrayList<Topology>();
        // ADDED BY FARIA

        ArrayList<Topology> successfulTopologies = new ArrayList<Topology>();

        for (Topology topology : stelaTopologies.values()) {
            long lastRebalancedAtTime = 0;
            if ( lastRebalancedAt.containsKey(topology.getId()) )
                lastRebalancedAtTime = lastRebalancedAt.get(topology.getId());
            if (topology.sloViolated() && (System.currentTimeMillis() / 1000 >=  lastRebalancedAtTime + REBALANCING_INTERVAL) && upForMoreThan(topology.getId()) ) {
                failingTopologies.add(topology);
               // lastRebalancedAt.put(topology.getId(), System.currentTimeMillis() / 1000);
            } else if ((System.currentTimeMillis() / 1000 >=  lastRebalancedAtTime + REBALANCING_INTERVAL) && upForMoreThan(topology.getId()) ) { // do the same for the
                successfulTopologies.add(topology);
            }
        }

        //Collections.sort(failingTopologies);
        //Collections.sort(successfulTopologies);

        TopologyPairs topologyPair = new TopologyPairs();
        topologyPair.setReceivers(failingTopologies);
        topologyPair.setGivers(successfulTopologies);

        return topologyPair;
    }

    public void updateLastRebalancedTime(String topologyId, Long time)
    {
        lastRebalancedAt.put(topologyId, time);
    }

    public void constructTopologyGraphs() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));

                List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();
                StringBuffer log = new StringBuffer();
                log.append("TOPOLOGY INFO \n");
                for (TopologySummary topologySummary : topologies) {

                log.append(topologySummary.get_id() + "\n");
                log.append("Topology Uptime: " +  nimbusClient.getClient().getTopologyInfo(topologySummary.get_id()).get_uptime_secs() + "\n");
                log.append("Topology Status: "  +  nimbusClient.getClient().getTopologyInfo(topologySummary.get_id()).get_status() + "\n");
                log.append("Topology Sched Status: " + nimbusClient.getClient().getTopologyInfo(topologySummary.get_id()).get_sched_status() + "\n");
                log.append("Topology Num Workers: " +  topologySummary.get_num_workers() + "\n");

                    String id = topologySummary.get_id();
                    StormTopology stormTopology = nimbusClient.getClient().getTopology(id);

                    if (!topologiesUptime.containsKey(id)) {
                        topologiesUptime.put(id, System.currentTimeMillis());
                    }

                    if (!stelaTopologies.containsKey(id) /*&& upForMoreThan(id)*/) {
                        Double userSpecifiedSlo = getUserSpecifiedSLOFromConfig(id);

                        Topology topology = new Topology(id, userSpecifiedSlo);
                        addSpoutsAndBolts(stormTopology, topology);
                        constructTopologyGraph(stormTopology, topology);

                        stelaTopologies.put(id, topology);

                    } else if (stelaTopologies.containsKey(id)){
                        TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(id);
                        updateParallelismHintsForTopology(topologyInfo, id, stormTopology);
                    }
                }

                log.append("********* END CLUSTER INFO **********\n");
                writeToFile(flatline_log, log.toString());

            } catch (NotAliveException e) {
                e.printStackTrace();
            } catch (TTransportException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean upForMoreThan(String id) {
        Long time = System.currentTimeMillis();
        Long topologyUpAt = topologiesUptime.get(id);

        return ((time - topologyUpAt) / 1000) > UP_TIME;
    }

    private Double getUserSpecifiedSLOFromConfig(String id) {
        Double topologySLO = 1.0;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));
            topologySLO = (Double) conf.get(Config.TOPOLOGY_SLO);
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        return topologySLO;
    }

    private void addSpoutsAndBolts(StormTopology stormTopology, Topology topology) {
        for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
            if (!spout.getKey().matches("(__).*")) {
                topology.addSpout(spout.getKey(), new Component(spout.getKey(),
                        spout.getValue().get_common().get_parallelism_hint()));
            }
        }

        for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                topology.addBolt(bolt.getKey(), new Component(bolt.getKey(),
                        bolt.getValue().get_common().get_parallelism_hint()));
            }
        }
    }

    private void constructTopologyGraph(StormTopology topology, Topology stelaTopology) {
        for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
            if (!bolt.getKey().matches("(__).*")) {
                Component component = stelaTopology.getBolts().get(bolt.getKey());

                for (Map.Entry<GlobalStreamId, Grouping> parent : bolt.getValue().get_common().get_inputs().entrySet()) {
                    String parentId = parent.getKey().get_componentId();

                    if (stelaTopology.getBolts().get(parentId) == null) {
                        stelaTopology.getSpouts().get(parentId).addChild(component.getId());
                    } else {
                        stelaTopology.getBolts().get(parentId).addChild(component.getId());
                    }

                    component.addParent(parentId);
                }
            }
        }
    }

    private void updateParallelismHintsForTopology(TopologyInfo topologyInfo, String topologyId, StormTopology stormTopology) {

        HashMap<String, Integer> parallelism_hints = new HashMap<>();
        List<ExecutorSummary> execSummary = topologyInfo.get_executors();
        for (int i = 0; i < execSummary.size(); i++) {
            if (parallelism_hints.containsKey(execSummary.get(i).get_component_id()))
                parallelism_hints.put(execSummary.get(i).get_component_id(), parallelism_hints.get(execSummary.get(i).get_component_id()) + 1);
            else
                parallelism_hints.put(execSummary.get(i).get_component_id(), 1);
        }


        Topology topology = stelaTopologies.get(topologyId);
        HashMap<String, Component> allComponents = topology.getAllComponents();

        if (topologyInfo.get_executors().size() == 0) {

            for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
                if (allComponents.containsKey(spout.getKey())) {
                    allComponents.get(spout.getKey()).updateParallelism(spout.getValue().get_common().
                            get_parallelism_hint());
                }
            }

            for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
                if (allComponents.containsKey(bolt.getKey())) {
                    allComponents.get(bolt.getKey()).updateParallelism(bolt.getValue().get_common().get_parallelism_hint());
                }
            }
        }
        else {
            for (Map.Entry<String, SpoutSpec> spout : stormTopology.get_spouts().entrySet()) {
                if (allComponents.containsKey(spout.getKey())) {
                    allComponents.get(spout.getKey()).updateParallelism(parallelism_hints.get(spout.getKey()));
                      }
            }

            for (Map.Entry<String, Bolt> bolt : stormTopology.get_bolts().entrySet()) {
                if (allComponents.containsKey(bolt.getKey())) {
                    allComponents.get(bolt.getKey()).updateParallelism(parallelism_hints.get(bolt.getKey()));
                }
            }
        }
    }


    public void remove(String topologyId) {
        stelaTopologies.remove(topologyId);
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.append(data);
            bufferWritter.close();
            fileWritter.close();
            //LOG.info("wrote to slo file {}",  data);
        } catch (IOException ex) {
            // LOG.info("error! writing to file {}", ex);
            System.out.println(ex.toString());
        }
    }
}
