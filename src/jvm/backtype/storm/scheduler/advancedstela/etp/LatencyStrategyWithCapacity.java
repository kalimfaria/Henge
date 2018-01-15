package backtype.storm.scheduler.advancedstela.etp;

import java.util.*;


public class LatencyStrategyWithCapacity {

    private TopologySchedule topologySchedule;

    public LatencyStrategyWithCapacity(TopologySchedule tS) {

        topologySchedule = tS;

    }

    public ArrayList<ResultComponent> topologyCapacityDescending()
    {
        HashMap<String, Component> components = topologySchedule.getComponents();
        List<Component> comp  = new ArrayList<Component>(components.values());
        Collections.sort(comp, Collections.reverseOrder());
        ArrayList <ResultComponent> resultComponent = new ArrayList<ResultComponent>();

        for (int i = 0; i < comp.size(); i++)
        {
            if (!comp.get(i).getId().contains("spout")) {
                ResultComponent rComp = new ResultComponent(comp.get(i), comp.get(i).getCapacity());
                resultComponent.add(rComp);
            }
        }
        return resultComponent;
    }

}
