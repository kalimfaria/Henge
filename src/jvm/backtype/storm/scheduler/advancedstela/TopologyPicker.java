package backtype.storm.scheduler.advancedstela;

import java.util.ArrayList;

/**
 * Created by fariakalim on 1/25/16.
 */
public class TopologyPicker {

    public ArrayList<String> bestTargetWorstVictim (ArrayList <String> receivers, ArrayList <String> givers)
// first index should be receiver. // second index should be giver
    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;
    }

    public ArrayList<String> bestTargetBestVictim (ArrayList <String> givers, ArrayList <String> receivers)

    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        topologyPair.add(givers.get(0));
        return topologyPair;
    }

    public ArrayList<String> worstTargetBestVictim (ArrayList <String> givers, ArrayList <String> receivers)

    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        topologyPair.add(givers.get(0));
        return topologyPair;

    }

    public ArrayList<String> worstTargetWorstVictim (ArrayList <String> givers, ArrayList <String> receivers)

    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;

    }
}
