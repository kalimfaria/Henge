package backtype.storm.scheduler.advancedstela.slo;

import java.util.ArrayList;

public class TopologyPairs {
    private ArrayList<String> givers;
    private ArrayList<String> receivers;

    public TopologyPairs() {
        givers = new ArrayList<String>();
        receivers = new ArrayList<String>();
    }

    public ArrayList<String> getGivers() {
        return givers;
    }

    public void setGivers(ArrayList<Topology> giverTopologies) {
        for (Topology topology: giverTopologies) {
            givers.add(topology.getId());
        }
    }

    public ArrayList<String> getReceivers() {
        return receivers;
    }

    public void setReceivers(ArrayList<Topology> receiverTopologies) {
        for (Topology topology: receiverTopologies) {
            receivers.add(topology.getId());
        }
    }
}
