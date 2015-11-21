package backtype.storm.scheduler.advancedstela.slo;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift.TException;
import org.apache.thrift7.transport.TTransportException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

public class Topologies {
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
        ArrayList<Topology> failingTopologies  = new ArrayList<Topology>();
        ArrayList<Topology> successfulTopologies  = new ArrayList<Topology>();

        for (Topology topology: stelaTopologies.values()) {
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

                    if (!stelaTopologies.containsKey(id)) {
                        Double userSpecifiedSlo = getUserSpecifiedSLOFromConfig(id);

                        Topology topology = new Topology(id, userSpecifiedSlo);
                        StormTopology stormTopology = nimbusClient.getClient().getTopology(id);

                        addSpoutsAndBolts(stormTopology, topology);
                        constructTopologyGraph(stormTopology, topology);

                        stelaTopologies.put(id, topology);
                    }
                }
            } catch (TException e) {
                e.printStackTrace();
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }

    private Double getUserSpecifiedSLOFromConfig(String id) throws TException {
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
        }
        return topologySLO;
    }

    private void addSpoutsAndBolts(StormTopology stormTopology, Topology topology) throws TException {
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

                for (Map.Entry<GlobalStreamId, Grouping> parent : bolt.getValue().get_common().get_inputs().entrySet())
                {
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
}
