package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertEventNotifyMetric;
import cloud.erda.analyzer.alert.models.AlertTrigger;
import cloud.erda.analyzer.alert.utils.OutputTagUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AlertLevelProcessFunction extends ProcessWindowFunction<AlertEvent, AlertEvent, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<AlertEvent> iterable, Collector<AlertEvent> collector) throws Exception {

        AlertEventNotifyMetric processMetric = null;
        long counter = 0;

        boolean recover = true;
        AlertEvent maxLevelAlert = null;
        AlertEvent minLevelAlert = null;
        for (AlertEvent alertEvent : iterable) {
            counter++;
            if (processMetric == null){
                processMetric = AlertEventNotifyMetric.createFrom(alertEvent.getMetricEvent());
            }
            if (alertEvent.getTrigger().equals(AlertTrigger.alert)) {
                recover = false;
                if (maxLevelAlert == null) {
                    maxLevelAlert = alertEvent;
                } else if (alertEvent.getLevel().compareTo(maxLevelAlert.getLevel()) <= 0) {
                    maxLevelAlert = alertEvent;
                }
            } else {
                if (minLevelAlert == null) {
                    minLevelAlert = alertEvent;
                } else if (minLevelAlert.getLevel().compareTo(alertEvent.getLevel()) <= 0) {
                    minLevelAlert = alertEvent;
                }
            }
        }
        if (recover) {
            collector.collect(minLevelAlert);
        } else {
            collector.collect(maxLevelAlert);
        }
        if (processMetric != null) {
            processMetric.addReduced(counter-1);
            context.output(OutputTagUtils.AlertEventNotifyProcess, processMetric);
        }
    }
}
