package cloud.erda.analyzer.runtime.functions;

import cloud.erda.analyzer.runtime.models.DiceOrg;
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import lombok.val;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.net.MalformedURLException;
import java.net.URL;

public class DiceOrgBroadcastProcessFunction extends BroadcastProcessFunction<ExpressionMetadata, DiceOrg,ExpressionMetadata> {

    private MapStateDescriptor<Long,String> diceOrgDescriptor;
    public DiceOrgBroadcastProcessFunction(MapStateDescriptor<Long,String> diceOrgDescriptor) {
        this.diceOrgDescriptor = diceOrgDescriptor;
    }

    @Override
    public void processElement(ExpressionMetadata expressionMetadata, ReadOnlyContext readOnlyContext, Collector<ExpressionMetadata> collector) throws Exception {
        if (expressionMetadata == null) {
            return;
        }
        val orgId = expressionMetadata.getAttributes().get("dice_org_id");
        ReadOnlyBroadcastState<Long,String> diceOrg = readOnlyContext.getBroadcastState(diceOrgDescriptor);
        String orgName = new String();
        if (orgId != null) {
            orgName = diceOrg.get((Long.parseLong(orgId)));
        }
        String displayUrl = expressionMetadata.getAttributes().get("display_url");
        String recordUrl = expressionMetadata.getAttributes().get("record_url");
        if (orgName != null) {
            if (displayUrl != null) {
                displayUrl = ModifyUrl(orgName,displayUrl);
                expressionMetadata.getAttributes().put("display_url",displayUrl);
            }
            if (recordUrl != null) {
                recordUrl = ModifyUrl(orgName,recordUrl);
                expressionMetadata.getAttributes().put("record_url",recordUrl);
            }
            collector.collect(expressionMetadata);
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
    public void processBroadcastElement(DiceOrg diceOrg, Context ctx, Collector<ExpressionMetadata> collector) throws Exception {
        if (diceOrg != null) {
            ctx.getBroadcastState(diceOrgDescriptor).put(diceOrg.getId(),diceOrg.getName());
        }
    }
}
