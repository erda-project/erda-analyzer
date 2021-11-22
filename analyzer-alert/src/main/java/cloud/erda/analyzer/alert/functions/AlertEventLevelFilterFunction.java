package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertLevel;
import cloud.erda.analyzer.common.constant.AlertConstants;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Arrays;

public class AlertEventLevelFilterFunction implements FilterFunction<AlertEvent> {

    @Override
    public boolean filter(AlertEvent alertEvent) throws Exception {
        AlertLevel eventLevel = alertEvent.getLevel();
        AlertLevel[] notifyLevels = alertEvent.getAlertNotify().getNotifyTarget().getLevels();

        // compatible only when the get level fails (AlertLevel.UNKNOWN) and the notifyGroup's level is empty
        if (AlertLevel.UNKNOWN.equals(eventLevel) || notifyLevels.length == 0) {
            return true;
        }

        for (AlertLevel notifyLevel : notifyLevels) {
            if (eventLevel.equals(notifyLevel))
                return true;
        }

        return false;
    }
}
