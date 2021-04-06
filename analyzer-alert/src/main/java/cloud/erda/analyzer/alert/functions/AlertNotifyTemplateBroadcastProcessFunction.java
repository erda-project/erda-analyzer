// Copyright (c) 2021 Terminus, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertNotifyTarget;
import cloud.erda.analyzer.alert.models.AlertNotifyTemplate;
import cloud.erda.analyzer.common.constant.AlertConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-01-05 21:13
 **/
@Slf4j
public class AlertNotifyTemplateBroadcastProcessFunction extends BroadcastProcessFunction<AlertEvent, AlertNotifyTemplate, AlertEvent> {

    private MapStateDescriptor<String, AlertNotifyTemplate> alertNotifyTemplateStateDescriptor;
    private long stateTtl;
    private long lastCleanTime;

    public AlertNotifyTemplateBroadcastProcessFunction(long stateTtl, MapStateDescriptor<String, AlertNotifyTemplate> alertNotifyTemplateStateDescriptor) {
        this.alertNotifyTemplateStateDescriptor = alertNotifyTemplateStateDescriptor;
        this.stateTtl = stateTtl;
    }

    @Override
    public void processElement(AlertEvent value, ReadOnlyContext ctx, Collector<AlertEvent> out) throws Exception {
        if (value == null) {
            return;
        }
        ReadOnlyBroadcastState<String, AlertNotifyTemplate> templateState = ctx.getBroadcastState(alertNotifyTemplateStateDescriptor);
        String[] templateKeys = getTemplateKeys(value, value.getAlertNotify().getNotifyTarget());
        for (String templateKey : templateKeys) {
            AlertNotifyTemplate template = templateState.get(templateKey);
            if (template != null) {
                value.setAlertNotifyTemplate(template);
                out.collect(value);
            }
        }
    }

    @Override
    public void processBroadcastElement(AlertNotifyTemplate value, Context ctx, Collector<AlertEvent> out) throws Exception {
        cleanExpireState(ctx);
        if (value == null) {
            return;
        }
        ctx.getBroadcastState(alertNotifyTemplateStateDescriptor).put(getTemplateKey(value), value);
    }

    private void cleanExpireState(Context ctx) throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCleanTime < stateTtl) {
            return;
        }

        lastCleanTime = now;

        BroadcastState<String, AlertNotifyTemplate> templates = ctx.getBroadcastState(alertNotifyTemplateStateDescriptor);
        if (templates == null) {
            return;
        }
        Map<String, AlertNotifyTemplate> immutableEntries = new HashMap<>();
        templates.immutableEntries().forEach(entry -> immutableEntries.put(entry.getKey(), entry.getValue()));
        for (Map.Entry<String, AlertNotifyTemplate> template : immutableEntries.entrySet()) {
            if (now - template.getValue().getProcessingTime() > stateTtl) {
                templates.remove(template.getKey());
            }
        }

        log.info("Clean up expired notify template.");
    }

    private String getTemplateKey(AlertNotifyTemplate value) {
        return value.getAlertType() + "." + value.getAlertIndex() + "." + value.getTarget() + "." + value.getTrigger().name();
    }

    private String[] getTemplateKeys(AlertEvent value, AlertNotifyTarget target) {
        if (AlertConstants.ALERT_NOTIFY_TYPE_NOTIFY_GROUP.equals(target.getType())) {
            String[] keys = new String[target.getGroupTypes().length];
            for (int i = 0; i < target.getGroupTypes().length; i++) {
                keys[i] = value.getAlertType() + "." + value.getAlertIndex() + "." + target.getGroupTypes()[i] + "." + value.getTrigger().name();
            }
            return keys;
        }
        return new String[]{value.getAlertType() + "." + value.getAlertIndex() + "." + target.getType() + "." + value.getTrigger().name()};
    }
}
