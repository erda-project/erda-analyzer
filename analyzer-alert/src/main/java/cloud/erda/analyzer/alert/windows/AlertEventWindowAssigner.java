package cloud.erda.analyzer.alert.windows;

import cloud.erda.analyzer.alert.models.AlertEvent;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

public class AlertEventWindowAssigner extends WindowAssigner<AlertEvent, TimeWindow> {
    @Override
    public Collection<TimeWindow> assignWindows(AlertEvent alertEvent, long timestamp, WindowAssignerContext windowAssignerContext) {
        long size = Time.minutes(Long.parseLong(alertEvent.getMetricEvent().getTags().get("window"))).toMilliseconds();
        if (timestamp > Long.MIN_VALUE) {
            System.out.println("wwwwwwwwindowassigner");
            long start = TimeWindow.getWindowStartWithOffset(timestamp,0,size);
            return Collections.singletonList(new TimeWindow(start,start+size));
        }else {
            throw new RuntimeException("alert level time window has exception");
        }
    }

    @Override
    public Trigger<AlertEvent, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return new AlertEventLevelTrigger();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
