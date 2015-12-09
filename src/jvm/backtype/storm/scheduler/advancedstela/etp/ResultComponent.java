package backtype.storm.scheduler.advancedstela.etp;

public class ResultComponent implements Comparable<ResultComponent>{
    public Component component;
    public Double etpValue;

    public  ResultComponent(Component comp, Double value) {
        this.component = comp;
        this.etpValue = value;
    }

    @Override
    public int compareTo(ResultComponent that) {
        return this.etpValue.compareTo(that.etpValue);
    }
}
