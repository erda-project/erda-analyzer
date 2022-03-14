package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.Org;
import cloud.erda.analyzer.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

import java.util.Map;

@Slf4j
public class OrgLocaleBroadcastProcessFunction extends BroadcastProcessFunction<AlertEvent, Org, AlertEvent> {
    private MapStateDescriptor<String, Org> orgLocaleStateDescriptor;
    private long stateTtl;
    private long lastCleanTime;

    private static final String defaultLocale = "zh-CN";

    public OrgLocaleBroadcastProcessFunction(long stateTtl, MapStateDescriptor<String, Org> orgLocaleStateDescriptor) {
        this.orgLocaleStateDescriptor = orgLocaleStateDescriptor;
        this.stateTtl = stateTtl;
    }

    @Override
    public void processElement(AlertEvent alertEvent, ReadOnlyContext readOnlyContext, Collector<AlertEvent> collector) throws Exception {
        if (alertEvent == null) {
            return;
        }
        ReadOnlyBroadcastState<String, Org> orgLocaleState = readOnlyContext.getBroadcastState(orgLocaleStateDescriptor);
        Map<String, String> tags = alertEvent.getMetricEvent().getTags();
        String orgName = tags.get("org_name");

        String locale = defaultLocale;
        if (StringUtil.isNotEmpty(orgName)) {
            Org org = orgLocaleState.get(orgName);
            if (org != null) {
                locale = org.getLocale();
            }
        }

        alertEvent.setLocale(locale);
        collector.collect(alertEvent);
    }

    @Override
    public void processBroadcastElement(Org org, Context context, Collector<AlertEvent> collector) throws Exception {
        if (org == null) {
            return;
        }
        context.getBroadcastState(orgLocaleStateDescriptor).put(org.getName(), org);
    }
}
