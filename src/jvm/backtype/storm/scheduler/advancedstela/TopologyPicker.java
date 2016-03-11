package backtype.storm.scheduler.advancedstela;

import java.util.ArrayList;



public class TopologyPicker {

    public ArrayList<String> bestTargetWorstVictim (ArrayList<String> receivers, ArrayList<String>  givers)

    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        //topologyPair.add(givers.get(givers.size() - 1));
        topologyPair.add(givers.get(0));
        return topologyPair;
    }

    public ArrayList<String> bestTargetBestVictim (ArrayList<String>  receivers, ArrayList<String>  givers)

    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(0));
        //topologyPair.add(givers.get(0));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;
    }

    public ArrayList<String> worstTargetBestVictim (ArrayList<String>  receivers, ArrayList<String>  givers)

    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        //topologyPair.add(givers.get(0));
        topologyPair.add(givers.get(givers.size() - 1));
        return topologyPair;

    }

    public ArrayList<String> worstTargetWorstVictim (ArrayList<String>  receivers, ArrayList<String>  givers)

    {
        ArrayList<String> topologyPair = new ArrayList<>();
        topologyPair.add(receivers.get(receivers.size() - 1));
        topologyPair.add(givers.get(0));
        return topologyPair;

    }
}
