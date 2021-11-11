package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertLevel;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

public class AlertLevelProcessFunction extends ProcessWindowFunction<AlertEvent, AlertEvent, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<AlertEvent> iterable, Collector<AlertEvent> collector) throws Exception {
        //    1.全是告警恢复，应该发送级别最低的恢复
        //    2.有告警有恢复或者全是告警，发送级别最高的告警
        if (iterable == null) {
            return;
        }
        boolean allRecover = true;
        AlertLevel maxLevel = AlertLevel.Light;
        AlertLevel minLevel = AlertLevel.Breakdown;
        AlertLevel level;
        String trigger;
        HashMap<AlertLevel, ArrayList<AlertEvent>> alertLevelEventMap = new HashMap<>();
        AlertEvent recoverLevelEvent = new AlertEvent();
        for (AlertEvent alertEvent : iterable) {
            trigger = alertEvent.getMetricEvent().getTags().get("trigger");
            if (trigger.equals("alert")) {
                allRecover = false;
                level = AlertLevel.valueOf(alertEvent.getMetricEvent().getTags().get("level"));
                if (maxLevel.compareTo(level) >= 0) {
                    maxLevel = level;
                    ArrayList<AlertEvent> eventList = alertLevelEventMap.get(maxLevel);
                    if (eventList == null) {
                        eventList = new ArrayList<>();
                    }
                    eventList.add(alertEvent);
                    alertLevelEventMap.put(maxLevel, eventList);
                }
            }
            if (allRecover) {
                if (minLevel.compareTo(AlertLevel.valueOf(trigger)) <= 0) {
                    recoverLevelEvent = alertEvent;
                }
            }
        }
        if (allRecover) {
            collector.collect(recoverLevelEvent);
        } else {
            for (AlertEvent alertEvent : alertLevelEventMap.get(maxLevel)) {
                collector.collect(alertEvent);
            }
        }
    }
}
