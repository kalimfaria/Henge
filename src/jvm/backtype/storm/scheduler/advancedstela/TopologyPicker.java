package backtype.storm.scheduler.advancedstela;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class TopologyPicker {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyPicker.class);
    private File juice_log;

    public TopologyPicker () {
        juice_log = new File("/tmp/output.log");
    }
    public ArrayList<Topology> pickTopology(ArrayList<Topology> receiver_topologies, ArrayList<BriefHistory> history) {
        String strategy_name = "WT";
        switch (strategy_name) {
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
        if (receiver_topologies.size() == 0) return null;
        return receiver_topologies;
    }

    public void removeTopologiesThatDoNotShowImprovementWithRebalancing(ArrayList<Topology> receiver_topologies,
                                                                        ArrayList<BriefHistory> history)
    // if any rebalance in the last hour did not lead to more than 5% improvement in utility, the topology will be removed
    {
        Long currentTime = System.currentTimeMillis();
        for (BriefHistory briefHistory : history) {
            if (currentTime <= (briefHistory.getTime() + 60 * 1000)) { // rebalance was performed in the last hour
                for (Iterator<Topology> iterator = receiver_topologies.iterator(); iterator.hasNext(); ) {
                    Topology t = iterator.next();
                    LOG.info("iterating over {} in receiver ", t.getId() );
                    if (briefHistory.getTopology().equals(t.getId())) { // matches name
                        double currentUtility = t.getCurrentUtility();
                        double oldUtility = briefHistory.getUtility();
                        double requiredUtility = t.getTopologyUtility();
                        LOG.info("current {} old {} required {} top {} ",currentUtility , oldUtility  ,requiredUtility, t.getId() );
                        if ((currentUtility - oldUtility) / requiredUtility < 0.05) // if improvement is less than 5%
                        {
                            LOG.info("Not choosing " + t.getId() + "\n");
                            writeToFile(juice_log, "Not choosing " + t.getId() + "\n");
                            iterator.remove();
                            // remove from topologies
                        }
                    }
                }
            }
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