package backtype.storm.scheduler.advancedstela.etp;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Node {
    public String supervisorId;
    public String hostname;
    public SupervisorDetails supervisorDetails;
    public List<WorkerSlot> slots;
    public List<ExecutorDetails> executors;
    public Map<WorkerSlot, List<ExecutorDetails>> slotsToExecutors;

    public Node(SupervisorDetails details, List<WorkerSlot> assignableSlots) {
        supervisorDetails = details;
        supervisorId = supervisorDetails.getId();
        hostname = supervisorDetails.getHost();
        slots = assignableSlots;
        executors = new ArrayList<ExecutorDetails>();

        slotsToExecutors = new HashMap<WorkerSlot, List<ExecutorDetails>>();
        for (WorkerSlot ws : slots) {
            slotsToExecutors.put(ws, new ArrayList<ExecutorDetails>());
        }
    }
}
