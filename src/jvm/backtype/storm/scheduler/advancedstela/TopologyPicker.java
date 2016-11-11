package backtype.storm.scheduler.advancedstela;
import backtype.storm.scheduler.advancedstela.slo.Topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class TopologyPicker {

    public Topology pickTopology(ArrayList<Topology> receiver_topologies, ArrayList<BriefHistory> history)
    {
        String strategy_name = "WT";
        switch(strategy_name)
        {
            case "WT": {
                Topology.sortingStrategy = "descending-specified-ascending-current-utility";
                Collections.sort(receiver_topologies);
               /* Topology.sortingStrategy = "ascending-current-utility";
                Collections.sort(receiver_topologies); */

                for (Topology t : receiver_topologies) {
                    System.out.println(t.getId() + " " + t.getCurrentUtility() + " " + t.getTopologyUtility());
                }
                break;
            }
            case "BT": {
                Topology.sortingStrategy = "ascending-current-utility";
                Collections.sort(receiver_topologies);
                break;
            }
        }

        removeTopologiesThatDoNotShowImprovementWithRebalancing(receiver_topologies, history);
        return receiver_topologies.get(0);
    }

    public void removeTopologiesThatDoNotShowImprovementWithRebalancing (ArrayList<Topology> receiver_topologies,
                                                                         ArrayList<BriefHistory> history)
    // if any rebalance in the last hour did not lead to more than 5% improvement in utility, the topology will be removed
    {
        Long currentTime = System.currentTimeMillis();
        for (BriefHistory briefHistory: history){
            if (currentTime <= (briefHistory.getTime() + 60 * 1000 )) { // rebalance was performed in the last hour
                for (Iterator<Topology> iterator = receiver_topologies.iterator(); iterator.hasNext();) {
                    Topology t = iterator.next();
                    if (briefHistory.getTopology().equals(t.getId())) { // matches name
                        double currentUtility = t.getCurrentUtility();
                        double oldUtility = briefHistory.getUtility();
                        double requiredUtility = t.getTopologyUtility();
                        if ((currentUtility - oldUtility)/requiredUtility < 0.05) // if improvement is less than 5%
                        {
                            iterator.remove();
                            // remove from topologies
                        }

                    }
                }
            }
        }
    }
}
