package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertLevel;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

public class AlertLevelProcessFunction extends ProcessWindowFunction<AlertEvent,AlertEvent,String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<AlertEvent> elements, Collector<AlertEvent> collector) throws Exception {
        AlertLevel minLevel = AlertLevel.Light;
        HashMap<AlertLevel, ArrayList<AlertEvent>> levelEventMap= new HashMap<>();
        String levelJson = JSONObject.toJSONString(elements);
        System.out.println("ppppppppppprocess alert event elements is "+levelJson);
        for (AlertEvent element : elements) {
            AlertLevel level = AlertLevel.valueOf(element.getMetricEvent().getTags().get("level"));
            if (level.compareTo(minLevel) <= 0) {
                minLevel = level;
                ArrayList<AlertEvent> eventList = levelEventMap.get(minLevel);
                if (eventList == null) {
                    eventList = new ArrayList<>();
                }
                eventList.add(element);
                levelEventMap.put(minLevel,eventList);
            }
        }
        for (AlertEvent alertEvent : levelEventMap.get(minLevel)) {
            String json = JSONObject.toJSONString(alertEvent);
            System.out.println("lllllllllllevel alert event is "+json);
            collector.collect(alertEvent);
        }
    }
}
