import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class Roll {
    private List<Timestamp> timestamps;
    private List<Double> speeds;
    private List<Double> amperes;
    private List<String> names;

    public Roll() {
        timestamps = new ArrayList<>();
        speeds = new ArrayList<>();
        amperes = new ArrayList<>();
        names = new ArrayList<>();
    }

    void addValues(Timestamp t, Double sp, Double amp, String n) {
        timestamps.add(t);
        speeds.add(sp);
        amperes.add(amp);
        names.add(n);
    }

    public List<Timestamp> getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(List<Timestamp> timestamps) {
        this.timestamps = timestamps;
    }

    public List<Double> getSpeeds() {
        return speeds;
    }

    public void setSpeeds(List<Double> speeds) {
        this.speeds = speeds;
    }

    public List<Double> getAmperes() {
        return amperes;
    }

    public void setAmperes(List<Double> amperes) {
        this.amperes = amperes;
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }
}
