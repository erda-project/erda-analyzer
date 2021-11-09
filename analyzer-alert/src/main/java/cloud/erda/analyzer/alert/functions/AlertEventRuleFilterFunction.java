package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import org.apache.flink.api.java.functions.KeySelector;

public class AlertEventRuleFilterFunction implements KeySelector<AlertEvent, String> {
    @Override
    public String getKey(AlertEvent alertEvent) throws Exception {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append("alert_id_").append(alertEvent.getAlertId())
                .append("rule_alert_index_").append(alertEvent.getAlertNotifyTemplate().getAlertIndex());
        System.out.println("kkkkkkkkkkkkkthe level key is "+keyBuilder.toString());
        return keyBuilder.toString();
    }
}
