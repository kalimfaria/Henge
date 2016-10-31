

import backtype.storm.scheduler.advancedstela.slo.Sensitivity;
import backtype.storm.scheduler.advancedstela.slo.Topology;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Created by fariakalim on 10/31/16.
 */
public class TestTopology {

    Topology t1;
    Topology t2;
    Topology t3;
    Topology t4;

    @Before
    public void setup () {
        t1 = new Topology("T1", 1.0, 0.0, 5.0, 5L); // throughput sensitive
        t2 = new Topology("T2", 0.0, 50.0, 25.0, 5L); // latency sensitive
        t3 = new Topology("T3", 1.0, 50.0, 35.0, 5L); // both
        t4 = new Topology("T4", 1.0, 0.0, 5.0, 5L); // throughput sensitive
    }

    @Test
    public void testSensitivityType() {
        Sensitivity s1 = t1.getSensitivity();
        Sensitivity s2 = t2.getSensitivity();
        Sensitivity s3 = t3.getSensitivity();
        Sensitivity s4 = t4.getSensitivity();

        assertEquals(Sensitivity.JUICE, s1);
        assertEquals(Sensitivity.LATENCY, s2);
        assertEquals(Sensitivity.BOTH, s3);
        assertEquals(Sensitivity.JUICE, s4);
    }

    @Test
    public void testUtilityCalculations() {
        t1.setMeasuredSLOs(0.5);
        t1.setMeasuredSLOs(1.0);
        Double measuredUtility = t1.getCurrentUtility();
        assertEquals((Double) 3.75, measuredUtility);
    }


}
