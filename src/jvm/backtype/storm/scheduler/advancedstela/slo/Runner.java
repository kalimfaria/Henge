package backtype.storm.scheduler.advancedstela.slo;

public class Runner implements Runnable {
    private Observer observer;

    public Runner(Observer observer) {
        this.observer = observer;
    }

    @Override
    public void run() {
        observer.run();
    }
}
