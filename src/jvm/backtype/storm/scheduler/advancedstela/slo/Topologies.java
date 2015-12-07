package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

public class Topologies {
    private static final Integer UP_TIME = 60;

    private Map config;
    private NimbusClient nimbusClient;
    private HashMap<String, Topology> stelaTopologies;

    public Topologies(Map conf) {
        config = conf;
        stelaTopologies = new HashMap<String, Topology>();
    }

    public HashMap<String, Topology> getStelaTopologies() {
        return stelaTopologies;
    }

    public TopologyPairs getTopologyPairScaling() {
        ArrayList<Topology> failingTopologies = new ArrayList<Topology>();
        ArrayList<Topology> successfulTopologies = new ArrayList<Topology>();

        for (Topology topology : stelaTopologies.values()) {
            if (topology.sloViolated()) {
                failingTopologies.add(topology);
            } else {
                successfulTopologies.add(topology);
            }
        }

        Collections.sort(failingTopologies);
        Collections.sort(successfulTopologies);

        TopologyPairs topologyPair = new TopologyPairs();
        topologyPair.setReceivers(failingTopologies);
        topologyPair.setGivers(successfulTopologies);

        return topologyPair;
    }

    public void constructTopologyGraphs() {
        if (config != null) {
            try {
                nimbusClient = new NimbusClient(config, (String) config.get(Config.NIMBUS_HOST));

                List<TopologySummary> topologies = nimbusClient.getClient().getClusterInfo().get_topologies();

                for (TopologySummary topologySummary : topologies) {
                    String id = topologySummary.get_id();
                    StormTopology stormTopology = nimbusClient.getClient().getTopology(id);
                    TopologyInfo topologyInfo = nimbusClient.getClient().getTopologyInfo(id);

                    if (!stelaTopologies.containsKey(id) && topologyInfo.get_uptime_secs() > UP_TIME) {
                        Double userSpecifiedSlo = getUserSpecifiedSLOFromConfig(id);

                        Topology topology = new Topology(id, userSpecifiedSlo);
                        addSpoutsAndBolts(stormTopology, topology);
                        constructTopologyGraph(stormTopology, topology);

                        stelaTopologies.put(id, topology);
                    } else if (stelaTopologies.containsKey(id) && topologyInfo.get_uptime_secs() < UP_TIME) {
                        stelaTopologies.remove(id);
                    } else if (stelaTopologies.containsKey(id) && topologyInfo.get_uptime_secs() > UP_TIME){
                        updateParallelismHintsForTopology(id, stormTopology);
                    }
                }
                
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

    private void updateParallelismHintsForTopology(String topologyId, StormTopology stormTopology) {
        Topology topology = stelaTopologies.get(topologyId);
        HashMap<String, Component> allComponents = topology.getAllComponents();

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
}
