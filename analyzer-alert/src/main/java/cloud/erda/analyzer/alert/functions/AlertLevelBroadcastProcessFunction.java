package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

@Slf4j
public class AlertLevelBroadcastProcessFunction extends BroadcastProcessFunction<AlertEvent,AlertEvent,AlertEvent> {
    private MapStateDescriptor<String, AlertEvent> alertLevelStateDescriptor;
    private long stateTtl;
    private long lastCleanTime;

    public AlertLevelBroadcastProcessFunction(long stateTtl,MapStateDescriptor<String, AlertEvent> alertLevelStateDescriptor) {
        this.alertLevelStateDescriptor = alertLevelStateDescriptor;
        this.stateTtl = stateTtl;
    }

    @Override
    public void processElement(AlertEvent alertEvent, ReadOnlyContext context, Collector<AlertEvent> collector) throws Exception {
        if (alertEvent == null) {
            return;
        }
        ReadOnlyBroadcastState<String,AlertEvent> alertLevelState =  context.getBroadcastState(alertLevelStateDescriptor);
        String levelKey = getAlertLevel(alertEvent);
        AlertEvent value = alertLevelState.get(levelKey);
        if (value != null) {
            collector.collect(alertEvent);
        }
    }

    @Override
    public void processBroadcastElement(AlertEvent alertEvent, Context context, Collector<AlertEvent> collector) throws Exception {
        if (alertEvent == null) {
            return;
        }
        cleanExpireState(context);
        context.getBroadcastState(alertLevelStateDescriptor).put(getAlertLevel(alertEvent),alertEvent);
    }

    public String getAlertLevel (AlertEvent value) {
        StringBuilder levelBuilder = new StringBuilder();
        levelBuilder.append("alert_id_").append(value.getAlertId())
                .append("rule_alert_index_").append(value.getAlertNotifyTemplate().getAlertIndex())
        .append("level_").append(value.getMetricEvent().getTags().get("level"));
        return levelBuilder.toString();
    }

    public void cleanExpireState(Context ctx) throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCleanTime < stateTtl) {
            return;
        }
        lastCleanTime = now;
        BroadcastState<String, AlertEvent> alertLevelMapState = ctx.getBroadcastState(alertLevelStateDescriptor);
        if (alertLevelMapState == null) {
            return;
        }
        for (Map.Entry<String,AlertEvent> item : alertLevelMapState.entries()) {
            if (now - item.getValue().getAlertNotify().getProcessingTime() > stateTtl) {
                alertLevelMapState.remove(item.getKey());
            }
        }
        log.info("clean up expired alert level");
    }
}
