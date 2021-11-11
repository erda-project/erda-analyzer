package cloud.erda.analyzer.alert.windows;

import cloud.erda.analyzer.alert.models.AlertEvent;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class AlertEventLevelTrigger extends Trigger<AlertEvent, TimeWindow> {
    @Override
    public TriggerResult onElement(AlertEvent alertEvent, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        if (timeWindow.maxTimestamp() <= triggerContext.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            triggerContext.registerEventTimeTimer(timeWindow.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return l == timeWindow.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteEventTimeTimer(timeWindow.maxTimestamp());
    }
}
