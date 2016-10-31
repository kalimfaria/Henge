package backtype.storm.scheduler.advancedstela.slo;

import java.util.HashMap;
import java.util.HashSet;

public class Component {
    private String id;
    private int parallelism;
    private HashSet<String> parents;
    private HashSet<String> children;
    private Integer totalTransferred;
    private Integer currentTransferred;

    private Long currentFailed;
    private Long prevFailed;
    private Long totalFailed;
    private Long currentAcked;
    private Long prevAcked;
    private Long totalAcked;

    private HashMap<String, Integer> totalExecuted;
    private HashMap<String, Integer> currentExecuted;
    private HashMap<String, Integer> currentExecuted_10Mins;
    private HashMap<String, Double> spoutTransfer;

    public Component(String key, int parallelismHint) {
        id = key;
        parallelism = parallelismHint;
        parents = new HashSet<String>();
        children = new HashSet<String>();
        totalTransferred = 0;
        currentTransferred = 0;
        currentFailed = prevFailed = totalFailed = 0L;
        currentAcked = prevAcked = totalAcked = 0L;
        totalExecuted = new HashMap<String, Integer>();
        currentExecuted = new HashMap<String, Integer>();
        currentExecuted_10Mins = new HashMap<String, Integer>();
        spoutTransfer = new HashMap<String, Double>();

    }

    public HashSet<String> getParents() {
        return parents;
    }

    public HashSet<String> getChildren() {
        return children;
    }

    public String getId() {
        return id;
    }

    public void addParent(String parentId) {
        parents.add(parentId);
    }

    public void addChild(String childId) {
        children.add(childId);
    }

    public void setTotalTransferred(Integer totalTransferred) {
        this.totalTransferred = totalTransferred;
    }

    public void setTotalFailed(Long totalFailed) {
        this.totalFailed = totalFailed;
    }

    public Integer getCurrentTransferred() {
        return currentTransferred;
    }

    public Long getCurrentFailed() {
        return this.currentFailed;
    }

    public Long getPrevFailed() {
        return this.prevFailed;
    }

    public Long getTotalFailed() {
        return this.totalFailed;
    }

    public void setCurrentFailed(Long currentFailed)  // send it total transferred
    {
        if (currentFailed < this.totalFailed) {
            this.totalFailed = 0L;
        }
        setPrevFailed(this.currentFailed);
        this.currentFailed = currentFailed - totalFailed;
    }

    public void setPrevFailed(Long prevFailed) {
        this.prevFailed = prevFailed;
    }

    public void setTotalAcked(Long totalAcked) {
        this.totalAcked = totalAcked;
    }

    public Long getCurrentAcked() {
        return this.currentAcked;
    }

    public Long getPrevAcked() {
        return this.prevAcked;
    }

    public Long getTotalAcked() {
        return this.totalAcked;
    }

    public void setCurrentAcked(Long currentAcked)  // send it total transferred
    {
        if (currentAcked < this.totalAcked) {
            this.totalAcked = 0L;
        }
        setPrevAcked(this.currentAcked);

        this.currentAcked = currentAcked - totalAcked;
    }

    public void setPrevAcked(Long prevAcked) {
        this.prevAcked = prevAcked;
    }


    public void setCurrentTransferred(Integer currentTransferred) {
        if (currentTransferred < totalTransferred) {
            totalTransferred = 0;
        }
        this.currentTransferred = currentTransferred - totalTransferred;
    }

    public HashMap<String, Integer> getCurrentExecuted() {
        return currentExecuted;
    }

    public Integer getTotalTransferred() {
        return totalTransferred;
    }

    public HashMap<String, Integer> getTotalExecuted() {
        return totalExecuted;
    }

    public HashMap<String, Double> getSpoutTransfer() {
        return spoutTransfer;
    }

    public void addSpoutTransfer(String key, Double value) {
        //   spoutTransfer.put(key, value);
        if (spoutTransfer.get(key) == null) {
            spoutTransfer.put(key, value);
        } else if (spoutTransfer.get(key) > 0.0) {
            spoutTransfer.put(key, spoutTransfer.get(key) + value);
        }
    }

    public void resetSpoutTransfer() {
        spoutTransfer = new HashMap<String, Double>();
    }

    public void addTotalExecuted(String key, Integer value) {
        totalExecuted.put(key, value);
    }

    public void addCurrentExecuted(String key, Integer value) {
        if (totalExecuted.get(key) == null) {

            currentExecuted.put(key, value);
        } else {
            if (value < totalExecuted.get(key)) {
                currentExecuted.put(key, value);

            } else {
                currentExecuted.put(key, value - totalExecuted.get(key));
            }
        }
    }

    public void setCurrentExecuted_10MINS(String key, Integer value) {
        currentExecuted_10Mins.put(key, value );
    }

    public HashMap<String, Integer> getCurrentExecuted_10MINS() {
        return currentExecuted_10Mins;
    }

    public void updateParallelism(int parallelismHint) {
        parallelism = parallelismHint;
    }

    public int getParallelism() {
        return parallelism;
    }

    public String printSLOValue() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (String component : spoutTransfer.keySet()) {
            sb.append(component).append(":").append(spoutTransfer.get(component));
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder p = new StringBuilder();
        p.append("[ ");
        for (String parent : parents) {
            p.append(parent).append(" ");
        }
        p.append("]");

        StringBuilder c = new StringBuilder();
        c.append("[ ");
        for (String child : children) {
            c.append(child).append(" ");
        }
        c.append("]");

        StringBuilder t = new StringBuilder();
        t.append("[ ");
        for (String component : totalExecuted.keySet()) {
            t.append(component).append(":").append(totalExecuted.get(component)).append(" ");
        }
        t.append("]");

        StringBuilder cE = new StringBuilder();
        cE.append("[ ");
        for (String component : currentExecuted.keySet()) {
            cE.append(component).append(":").append(currentExecuted.get(component)).append(" ");
        }
        cE.append("]");

        return "Component{" +
                "id='" + id + '\'' +
                ", parallelism=" + parallelism +
                ", parents=" + p.toString() +
                ", children=" + c.toString() +
                ", totalTransferred=" + totalTransferred +
                ", currentTransferred=" + currentTransferred +
                ", totalExecuted=" + t.toString() +
                ", currentExecuted=" + cE.toString() +
                ", sloValue=" + printSLOValue() +
                '}';
    }
}