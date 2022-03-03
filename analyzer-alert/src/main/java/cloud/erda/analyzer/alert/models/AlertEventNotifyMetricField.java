package cloud.erda.analyzer.alert.models;

import lombok.Data;

import java.io.Serializable;

@Data
public class AlertEventNotifyMetricField implements Serializable {

    private long reduced;

    private long silenced;

    public void addReduced(long delta) {
        reduced++;
    }

    public void addSilenced(long delta) {
        silenced++;
    }
}
