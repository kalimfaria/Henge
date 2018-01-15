package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Topologies {
    public static  Integer UP_TIME = 60 * 15;
    private static final Integer REBALANCING_INTERVAL = 60 * 5;//2;
    private static final Logger LOG = LoggerFactory.getLogger(Topologies.class);
    private Map config;
    private NimbusClient nimbusClient;
    private HashMap<String, Topology> stelaTopologies;
    private HashMap<String, Long> topologiesUptime;
    private HashMap<String, Long> lastRebalancedAt;

    private String folderName;

    public Topologies(Map conf) {
        config = conf;
        stelaTopologies = new HashMap<String, Topology>();
        topologiesUptime = new HashMap<String, Long>();
        lastRebalancedAt = new HashMap<String, Long>();


        String hostname = "Unknown";
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException ex) {
            System.out.println("Hostname can not be resolved");
        }

        if (hostname.equals("zookeepernimbus.storm-cluster-copy2.stella.emulab.net"))
            folderName = "/proj/Stella/latency-logs3/";
        else if (hostname.equals("zookeepernimbus.storm-cluster.stella.emulab.net"))
            folderName = "/proj/Stella/latency-logs1/";
        else if (hostname.equals("zookeepernimbus.storm-cluster-copy.stella.emulab.net"))
            folderName = "/proj/Stella/latency-logs2/";
        else if (hostname.equals("zookeepernimbus.advanced-stela.stella.emulab.net"))
            folderName = "/proj/Stella/latency-logs/";
        else if (hostname.equals("zookeepernimbus.stelaadvanced.stella.emulab.net"))
            folderName = "/proj/Stella/logs/";
        else if (hostname.equals("zookeepernimbus.hengeexperiment.stella.emulab.net"))
            folderName = "/proj/Stella/latency-hengeexperiment/";
    }

    public HashMap<String, Topology> getStelaTopologies() {
        return stelaTopologies;
    }

    public ArrayList<Topology> getFailingTopologies() { // when trying to add topologies to either of these

        // when clearing topology SLO, mark the time
        // when adding topologies back, I can check if that old time is greater than that time + the amount I want to stagger it for
        ArrayList<Topology> failingTopologies = new ArrayList<Topology>();

        ArrayList<Topology> successfulTopologies = new ArrayList<Topology>();

        for (Topology topology : stelaTopologies.values()) {
            long lastRebalancedAtTime = 0;
            if (lastRebalancedAt.containsKey(topology.getId()))
                lastRebalancedAtTime = lastRebalancedAt.get(topology.getId());


            if ((System.currentTimeMillis() / 1000 >= lastRebalancedAtTime + REBALANCING_INTERVAL) && upForMoreThan(topology.getId())) {
                LOG.info("The topology can be successful or failed \n");
                LOG.info("Topology name: " + topology.getId() + "\n");
                boolean violated = topology.sloViolated();

                if (violated) {
                    LOG.info(topology.getId() + " violated the SLO \n");
                    failingTopologies.add(topology);
                } else if (!violated) {
                    LOG.info(topology.getId() + " did not violate the SLO \n");
                    successfulTopologies.add(topology);
                }
            }
        }

        LOG.info("Failing Topologies: \n");
        for (Topology t : failingTopologies)
            LOG.info("Failing : " + t.getId() + "\n");
        LOG.info("Successful Topologies: \n");
        for (Topology t : successfulTopologies)
            LOG.info("Successful : " + t.getId() + "\n");


        LOG.info("Checking after topologies are set into the variables\n");


        return failingTopologies;
    }

    public ArrayList<Topology> getSuccessfulTopologies() { // when trying to add topologies to either of these

        ArrayList<Topology> successfulTopologies = new ArrayList<Topology>();
        for (Topology topology : stelaTopologies.values()) {
            long lastRebalancedAtTime = 0;
            if (lastRebalancedAt.containsKey(topology.getId()))
                lastRebalancedAtTime = lastRebalancedAt.get(topology.getId());
            if ((System.currentTimeMillis() / 1000 >= lastRebalancedAtTime + REBALANCING_INTERVAL) && upForMoreThan(topology.getId())) {
                LOG.info("The topology can be successful or failed \n");
                LOG.info("Topology name: " + topology.getId() + "\n");
                boolean violated = topology.sloViolated();
                if (!violated) {
                    LOG.info(topology.getId() + " did not violate the SLO \n");
                    successfulTopologies.add(topology);
                }
            }
        }
        LOG.info("Successful Topologies: \n");
        for (Topology t : successfulTopologies)
            LOG.info("Successful : " + t.getId() + "\n");
        return successfulTopologies;
    }

    public void updateLastRebalancedTime(String topologyId, Long time) {
        lastRebalancedAt.put(topologyId, time);
    }

    public void constructTopologyGraphs() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));

                List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();

                for (TopologySummary topologySummary : topologies) {


                    String id = topologySummary.get_id();
                    StormTopology stormTopology = nimbusClient.getClient().getTopology(id);

                    if (!topologiesUptime.containsKey(id)) {
                        topologiesUptime.put(id, System.currentTimeMillis());
                    }

                    if (!stelaTopologies.containsKey(id) /*&& upForMoreThan(id)*/) {
                        //String sortingStrategy = "Unified"; // other - Class-Based
                        Double userSpecifiedSlo = getUserSpecifiedSLOFromConfig(id);
                        Double userLatencySLO = getUserSpecifiedLatencySLOFromConfig(id);
                        Double utility = getUserSpecifiedUtilityFromConfig(id);
                        // String sensitivity = getUserSLOSensitivityFromConfig(id);
                        Long numWorkers = getNumWorkersFromConfig(id);
                        Topology topology = new Topology(id, userSpecifiedSlo, userLatencySLO, utility, numWorkers);

                        addSpoutsAndBolts(stormTopology, topology);
                        constructTopologyGraph(stormTopology, topology);

                        stelaTopologies.put(id, topology);

                    } else if (stelaTopologies.containsKey(id)) {
                        TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(id);
                        updateParallelismHintsForTopology(topologyInfo, id, stormTopology);
                        updateNumWorkersForTopology(topologySummary, id);
                    }
                }

                getLatencies();


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

        return (topologySLO == null ? 1.0 : topologySLO);
        //return topologySLO;
    }

    private Long getNumWorkersFromConfig(String id) {
        Long workers = 0L;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));
            workers = (Long) conf.get(Config.TOPOLOGY_WORKERS);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return workers;
    }

    private Double getUserSpecifiedLatencySLOFromConfig(String id) {
        Double topologyLatencySLO = 1.0;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));

            topologyLatencySLO = (Double) conf.get(Config.TOPOLOGY_LATENCY_SLO);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        //   return topologyLatencySLO;
        return (topologyLatencySLO == null ? 0.0 : topologyLatencySLO);
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

    private Double getUserSpecifiedUtilityFromConfig(String id) {
        Double topologyUtility = 0.0;
        JSONParser parser = new JSONParser();
        try {
            Map conf = (Map) parser.parse(nimbusClient.getClient().getTopologyConf(id));

            topologyUtility = (Double) conf.get(Config.TOPOLOGY_UTILITY);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        //   return topologyLatencySLO;
        return (topologyUtility == null ? 50.0 : topologyUtility);
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
        } else {
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


    private void updateNumWorkersForTopology(TopologySummary topologySummary, String topologyId) {
        Long numWorkers = new Long(topologySummary.get_num_workers());
        stelaTopologies.get(topologyId).setWorkers(numWorkers);
    }


    public void remove(String topologyId) {
        stelaTopologies.remove(topologyId);
    }

    public void getLatencies() {
        Latencies latencies_fetcher = new Latencies();
        HashMap<String, HashMap<HashMap<String, String>, ArrayList<Double>>> data = latencies_fetcher.getLatencies(); //  ReadFiles();
        for (String topology : stelaTopologies.keySet()) {
            Topology topology_ = stelaTopologies.get(topology);


            if (data.containsKey(topology)) {
                DescriptiveStatistics latencies = new DescriptiveStatistics();
                HashMap<HashMap<String, String>, ArrayList<Double>> top_data = data.get(topology);
                for (HashMap<String, String> op_pairs : top_data.keySet()) {

                    ArrayList<Double> times = top_data.get(op_pairs);
                    Double final_time = 0.0;
                    for (int i = 0; i < times.size(); i++) {
                        final_time += times.get(i);
                        latencies.addValue(times.get(i));
                    }
                    topology_.latencies.put(op_pairs, final_time / times.size()); //latencies.getMean()

                }
                LOG.info("Topology: " + topology + " average latency: " + latencies.getMean()
                        + " tail latency: " + latencies.getPercentile(99)
                        + " 75th percentile" + latencies.getPercentile(75) +
                        " 50th percentile" + latencies.getPercentile(50) + ". \n");

                topology_.setAverageLatency(latencies.getMean());
                topology_.set99PercentileLatency(latencies.getPercentile(99));
                topology_.set75PercentileLatency(latencies.getPercentile(75));
                topology_.set50PercentileLatency(latencies.getPercentile(50));
            } else {
                LOG.info("No latency data for topology so adding MAX");
                topology_.setAverageLatency(Double.MAX_VALUE);
                topology_.set99PercentileLatency(Double.MAX_VALUE);
                topology_.set75PercentileLatency(Double.MAX_VALUE);
                topology_.set50PercentileLatency(Double.MAX_VALUE);

            }
        }

    }
}