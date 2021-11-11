package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.common.constant.AlertConstants;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Arrays;

public class AlertEventLevelFilterFunction implements FilterFunction<AlertEvent> {

    @Override
    public boolean filter(AlertEvent alertEvent) throws Exception {
        String eventLevel = alertEvent.getMetricEvent().getTags().get(AlertConstants.ALERT_EXPRESSION_LEVEL);
        String[] notifyLevels = alertEvent.getAlertNotify().getNotifyTarget().getLevels();

        if (eventLevel == null || notifyLevels == null || notifyLevels.length == 0)
            return true;

        int len = notifyLevels.length;
        for (int i = 0; i < len; i++) {
            if (eventLevel.equals(notifyLevels[i]))
                return true;
        }

        return false;
    }
}
