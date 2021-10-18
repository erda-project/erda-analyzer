package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Arrays;

public class AlertEventLevelFilterFunction implements FilterFunction<AlertEvent> {

    @Override
    public boolean filter(AlertEvent alertEvent) throws Exception {
        String eventLevel = alertEvent.getMetricEvent().getTags().get("level");
        String[] notifyLevels = alertEvent.getAlertNotify().getNotifyTarget().getLevel();
        return eventLevel == null || notifyLevels == null || notifyLevels.length == 0
                || Arrays.stream(notifyLevels).anyMatch(level -> level == eventLevel);
    }
}
