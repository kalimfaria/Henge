

import backtype.storm.scheduler.advancedstela.BriefHistory;
import backtype.storm.scheduler.advancedstela.TopologyPicker;
import backtype.storm.scheduler.advancedstela.slo.Sensitivity;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by fariakalim on 10/31/16.
 */
public class TestTopology {

    Topology t1;
    Topology t2;
    Topology t3;

    @Before
    public void setup () {
        t1 = new Topology("T1", 1.0, 0.0, 5.0, 5L); // throughput sensitive
        t2 = new Topology("T2", 0.0, 50.0, 25.0, 5L); // latency sensitive
        t3 = new Topology("T3", 1.0, 50.0, 35.0, 5L); // both
    }

    @Test
    public void testSensitivityType() {
        Sensitivity s1 = t1.getSensitivity();
        Sensitivity s2 = t2.getSensitivity();
        Sensitivity s3 = t3.getSensitivity();

        assertEquals(Sensitivity.JUICE, s1);
        assertEquals(Sensitivity.LATENCY, s2);
        assertEquals(Sensitivity.BOTH, s3);
    }

    @Test
     public void testJuiceUtilityCalculations() {
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);
        Double measuredUtility = t1.getCurrentUtility();
        assertEquals((Double) 3.75, measuredUtility);
    }

    @Test
    public void testLatencyUtilityCalculations() {
        t2.setAverageLatency(70.0);
        Double measuredUtility = t2.getCurrentUtility();
        assertEquals((Double) 17.857142857142858, measuredUtility);
    }

    @Test
    public void testBothUtilityCalculations() {
        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        Double measuredUtility = t3.getCurrentUtility();
        assertEquals((Double) (25.0 * 0.5  + 0.5 * 26.25), measuredUtility);
    }

    @Test
    public void testSloViolated() {

        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        assertTrue(t1.sloViolated());

        t2.setAverageLatency(70.0);
        assertTrue(t2.sloViolated());

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        assertTrue(t3.sloViolated());
    }

    @Test
    public void testWhichSensitivityToHandleFirst() {
        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        assertEquals(Sensitivity.JUICE, t3.whichSLOToSatisfyFirst());
        t3.setAverageLatency(51.0);
        assertEquals(Sensitivity.LATENCY, t3.whichSLOToSatisfyFirst());
        t3.setAverageLatency(49.0);
        assertEquals(Sensitivity.JUICE, t3.whichSLOToSatisfyFirst());
        t3.setAverageLatency(60.0);
        for (int i = 0; i < 40 ; i++)
            t3.setMeasuredSLOs(1.0);
        assertEquals(Sensitivity.LATENCY, t3.whichSLOToSatisfyFirst());
    }

    @Test
    public void testSort() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        list.add(t3);
        list.add(t1);
        list.add(t2);
        Collections.sort(list);
        assertEquals(list.get(0).getId(), "T1");
        assertEquals(list.get(1).getId(), "T2");
        assertEquals(list.get(2).getId(), "T3");
        Topology.sortingStrategy = "descending";
        Collections.sort(list);
        assertEquals(list.get(0).getId(), "T3");
        assertEquals(list.get(1).getId(), "T2");
        assertEquals(list.get(2).getId(), "T1");
    }

    @Test
    public void testPickTopologySimple() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        list.add(t3);
        list.add(t1);
        list.add(t2);

        TopologyPicker picker = new TopologyPicker();
        Topology topology = picker.pickTopology(list, new ArrayList<BriefHistory>());
        assertEquals(topology.getId(), "T3");

    }

    @Test
    public void testPickTopologyRemoveRebalanced() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        list.add(t3);
        list.add(t1);
        list.add(t2);

        TopologyPicker picker = new TopologyPicker();
        BriefHistory history = new BriefHistory("T3", System.currentTimeMillis(), 26.25);
        ArrayList<BriefHistory> briefHistory = new ArrayList<BriefHistory>();
        briefHistory.add(history);
        Topology topology = picker.pickTopology(list, briefHistory);
        assertEquals(topology.getId(), "T2");

    }


    @Test
    public void testPickTopologyNotRemoveRebalanced() {
        ArrayList<Topology> list = new ArrayList<>();
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);

        t2.setAverageLatency(70.0);

        t3.setAverageLatency(70.0);
        t3.setMeasuredSLOs(0.5);
        t3.setMeasuredSLOs(1.0);
        list.add(t3);
        list.add(t1);
        list.add(t2);

        TopologyPicker picker = new TopologyPicker();
        BriefHistory history = new BriefHistory("T3", System.currentTimeMillis(), 10.0);
        ArrayList<BriefHistory> briefHistory = new ArrayList<BriefHistory>();
        briefHistory.add(history);
        Topology topology = picker.pickTopology(list, briefHistory);
        assertEquals(topology.getId(), "T3");

    }

}
