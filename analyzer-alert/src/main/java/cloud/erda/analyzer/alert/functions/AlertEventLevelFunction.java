package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import lombok.Data;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//相同策略相同规则只留等级最高
public class AlertEventLevelFunction extends KeyedProcessFunction<String, AlertEvent,AlertEvent> {
    private ValueStateDescriptor<LevelState> levelStateDescriptor;

    public AlertEventLevelFunction() {
        levelStateDescriptor = new ValueStateDescriptor<LevelState>("alert-event-level-state", TypeInformation.of(LevelState.class));
    }

    @Override
    public void processElement(AlertEvent alertEvent, Context context, Collector<AlertEvent> collector) throws Exception {
        ValueState<LevelState> state = getRuntimeContext().getState(levelStateDescriptor);
        LevelState level = state.value();
        if (level == null) {
            level = new LevelState();
            String eventLevel = alertEvent.getMetricEvent().getTags().get("level");
            level.setLevel(eventLevel);
            state.update(level);
        }
        if (AlertLevel.valueOf(level.level).compareTo(AlertLevel.valueOf(alertEvent.getMetricEvent().getTags().get("level"))) > 0) {
            return;
        }
        collector.collect(alertEvent);
        level.level = alertEvent.getMetricEvent().getTags().get("level");
        System.out.println("levellevellevel"+level);
        state.update(level);
    }

    @Data
    public static class LevelState {
        private String level;
    }

    public enum AlertLevel {
        fatal,
        critical,
        warning,
        info
    }
}
