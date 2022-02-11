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

import cloud.erda.analyzer.alert.models.AlertLevel;
import cloud.erda.analyzer.alert.templates.TemplateManager;
import cloud.erda.analyzer.alert.templates.TemplateRenderer;
import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertTrigger;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-01-05 16:47
 **/
@Slf4j
public class AlertEventMapFunction implements FlatMapFunction<MetricEvent, AlertEvent> {

    private TemplateManager templateManager = new TemplateManager();

    private static final HashSet<String> defaultCustomAlertTypes = new HashSet<>();

    static {
        defaultCustomAlertTypes.add("org_customize");
        defaultCustomAlertTypes.add("micro_service_customize");
    }

    @Override
    public void flatMap(MetricEvent value, Collector<AlertEvent> out) {
        Map<String, String> tags = value.getTags();
        String alertId = tags.get(AlertConstants.ALERT_ID);
        if (StringUtils.isEmpty(alertId)) {
            return;
        }
        String expressionId = tags.get(AlertConstants.ALERT_EXPRESSION_ID);
        if (StringUtils.isEmpty(expressionId)) {
            return;
        }
        String alertType = tags.get(AlertConstants.ALERT_TYPE);
        if (StringUtils.isEmpty(alertType)) {
            return;
        }
        String alertIndex = tags.get(AlertConstants.ALERT_INDEX);
        if (StringUtils.isEmpty(alertIndex)) {
            return;
        }
        String trigger = tags.get(AlertConstants.TRIGGER);
        if (StringUtils.isEmpty(trigger)) {
            return;
        }
        if( Boolean.parseBoolean(tags.get(AlertConstants.ALERT_SUPPRESSED))) {
            return;
        }
        try {
            AlertEvent alertEvent = new AlertEvent();
            alertEvent.setMetricEvent(value);
            alertEvent.setAlertId(alertId);
            alertEvent.setExpressionId(expressionId);
            alertEvent.setAlertType(alertType);
            alertEvent.setAlertIndex(alertIndex);
            alertEvent.setTrigger(AlertTrigger.valueOf(trigger));
            alertEvent.setLevel(AlertLevel.of(value));
            this.setAlertSource(alertEvent, alertType);
            // 设置分组ID
            this.setGroupId(alertEvent, alertType, alertIndex);
            out.collect(alertEvent);
        } catch (Throwable t) {
            log.error("Map alert event fail.", t);
        }
    }

    /**
     * 设置分组ID
     *
     * @param value 告警事件
     */
    private void setGroupId(AlertEvent value, String alertType, String alertIndex) {
        String alertGroup = value.getMetricEvent().getTags().get(AlertConstants.ALERT_GROUP);
        if (!StringUtils.isEmpty(alertGroup)) {
            TemplateRenderer renderer = templateManager.getRenderer(
                    AlertConstants.ALERT_GROUP_ID + alertGroup, alertGroup, false);
            alertGroup = renderer.render(new HashMap<>(value.getMetricEvent().getTags()));
        } else {
            alertGroup = String.format("%s_%s_%s", alertType, alertIndex, value.getMetricEvent().getTags().get(MetricTagConstants.METRIC_EXPRESSION_GROUP));
        }
        String groupId = value.getAlertId() + "_"
                + value.getAlertIndex() + "_"
                + new String(Base64.getUrlEncoder().encode(alertGroup.getBytes()), StandardCharsets.UTF_8)
                .toLowerCase().replace("=", "");
        value.setAlertGroup(groupId);
        value.getMetricEvent().getTags().put(AlertConstants.ALERT_GROUP_ID, groupId);
        value.getMetricEvent().getTags().put(AlertConstants.ALERT_GROUP, alertGroup);
    }

    private void setAlertSource(AlertEvent value, String alertType) {
        String alertSource = value.getMetricEvent().getTags().get(AlertConstants.ALERT_SOURCE);
        if(!StringUtils.isEmpty(alertSource))
            return;
        alertSource = defaultCustomAlertTypes.contains(alertType) ? "Custom" : "System";
        value.setAlertSource(alertSource);
        value.getMetricEvent().getTags().put(AlertConstants.ALERT_SOURCE, alertSource);
    }
}
