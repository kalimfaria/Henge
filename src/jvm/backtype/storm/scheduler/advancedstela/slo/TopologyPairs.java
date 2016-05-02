package backtype.storm.scheduler.advancedstela.slo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

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
        for (Topology topology : giverTopologies) {
            givers.add(topology.getId());
        }
    }

    public ArrayList<String> getReceivers() {
        return receivers;
    }

    public void setReceivers(ArrayList<Topology> receiverTopologies) {
        for (Topology topology : receiverTopologies) {
            receivers.add(topology.getId());
        }
    }
}

    /*public void setGivers(ArrayList<Topology> giverTopologies) {
        System.out.println("In setGivers");
        for (Topology topology: giverTopologies) {
            System.out.println("Topology ID:" + topology.getId());
            System.out.println("Topology User Specified SLO:" + topology.getUserSpecifiedSLO());
            System.out.println("Topology Measured SLO:" + topology.getMeasuredSLO());
            givers_temp.put(topology.getId(), Math.abs(topology.getUserSpecifiedSLO() - topology.getMeasuredSLO()));
        }


        List list = new LinkedList(givers_temp.entrySet());

        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {

                System.out.println(((Comparable) ((HashMap.Entry) (o1)).getValue()).compareTo(((HashMap.Entry) (o2)).getValue()));
                return ((Comparable) ((HashMap.Entry) (o1)).getValue())
                        .compareTo(((HashMap.Entry) (o2)).getValue());
            }
        });

        StringBuffer log = new StringBuffer();
        log.append("The sorted order of givers: \n");
        for (int i = 0; i < list.size(); i++)
        {
            givers.add((String) ((HashMap.Entry)list.get(i)).getKey());
            log.append((String) ((HashMap.Entry)list.get(i)).getKey() + " " + (Double) ((HashMap.Entry)list.get(i)).getValue()+ "\n");
        }

        log.append("***");
        writeToFile(flatline_log,log.toString());
    }*/



    /*public void setReceivers(ArrayList<Topology> receiverTopologies) {
        for (Topology topology: receiverTopologies) {
            receivers_temp.put(topology.getId(), Math.abs(topology.getUserSpecifiedSLO() - topology.getMeasuredSLO()));
        }

        List list = new LinkedList(receivers_temp.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {

                System.out.println(((Comparable) ((HashMap.Entry) (o1)).getValue()).compareTo(((HashMap.Entry) (o2)).getValue()));
                return ((Comparable) ((HashMap.Entry) (o1)).getValue())
                        .compareTo(((HashMap.Entry) (o2)).getValue());
            }
        });

        StringBuffer log = new StringBuffer();
        for (HashMap.Entry receiver : receivers_temp.entrySet())
        {
            receivers.add((String)receiver.getKey());
            log.append(receiver.getKey() + " " + receiver.getValue() + "\n");

        }
        log.append("***");
        writeToFile(flatline_log, log.toString());
    }*/

