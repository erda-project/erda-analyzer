package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.DiceOrg;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.val;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.net.MalformedURLException;
import java.net.URL;

public class DiceOrgBroadcastProcessFunction extends BroadcastProcessFunction<MetricEvent, DiceOrg,MetricEvent> {

    private MapStateDescriptor<Long,String> diceOrgDescriptor;
    public DiceOrgBroadcastProcessFunction(MapStateDescriptor<Long,String> diceOrgDescriptor) {
        this.diceOrgDescriptor = diceOrgDescriptor;
    }

    @Override
    public void processElement(MetricEvent metricEvent, ReadOnlyContext readOnlyContext, Collector<MetricEvent> collector) throws Exception {
        if (metricEvent == null) {
            return;
        }
        String orgName = metricEvent.getTags().get("org_name");
        ReadOnlyBroadcastState<Long,String> diceOrg = readOnlyContext.getBroadcastState(diceOrgDescriptor);
        if (orgName == null) {
            String orgId = metricEvent.getTags().get("dice_org_id");
            if (orgId != null) {
                orgName = diceOrg.get(Long.parseLong(orgId));
            }
        }
        if (orgName != null) {
            String displayUrl = metricEvent.getTags().get("display_url");
            String recordUrl = metricEvent.getTags().get("record_url");
            if (displayUrl != null) {
                displayUrl = ModifyUrl(orgName,displayUrl);
                metricEvent.getTags().put("display_url",displayUrl);
            }
            if (recordUrl != null) {
                recordUrl = ModifyUrl(orgName,recordUrl);
                metricEvent.getTags().put("record_url",recordUrl);
            }
            collector.collect(metricEvent);
        }
    }

    public String ModifyUrl(String orgName,String url) throws MalformedURLException {
        URL u = new URL(url);
        String protocol = u.getProtocol();
        String host = u.getHost();
        StringBuffer stringBuffer = new StringBuffer(url);
        String head = protocol + "://" + host + "/";
        String subString = url.substring(head.length()-1, url.length()-head.length()-1);
        val elements = subString.split("/");
        if (!elements[0].equals(orgName)) {
            stringBuffer.insert(head.length(),orgName+"/");
            return stringBuffer.toString();
        }
        return url;
    }

    @Override
    public void processBroadcastElement(DiceOrg diceOrg, Context ctx, Collector<MetricEvent> collector) throws Exception {
        if (diceOrg != null) {
            ctx.getBroadcastState(diceOrgDescriptor).put(diceOrg.getId(),diceOrg.getName());
        }
    }
}
