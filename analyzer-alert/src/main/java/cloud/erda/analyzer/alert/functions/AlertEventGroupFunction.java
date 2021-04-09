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

import cloud.erda.analyzer.alert.templates.TemplateManager;
import cloud.erda.analyzer.alert.templates.TemplateRenderer;
import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.common.constant.AlertConstants;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.HashMap;

/**
 * @author: liuhaoyang
 * @create: 2020-01-05 21:49
 **/
public class AlertEventGroupFunction implements KeySelector<AlertEvent, String> {

    private TemplateManager templateManager = new TemplateManager();

    @Override
    public String getKey(AlertEvent value) throws Exception {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append("alert_id_").append(value.getAlertId()).append("_")
                .append("notify_template_id_").append(value.getAlertNotifyTemplate().getId()).append("_")
                .append("expression_id_").append(value.getMetricEvent().getTags().get(AlertConstants.ALERT_EXPRESSION_ID))
                .append("_");
        TemplateRenderer templateRenderer = templateManager.getRenderer(
                AlertConstants.ALERT_GROUP_ID + value.getAlertGroup(), value.getAlertGroup(), value.getAlertNotifyTemplate().isVariable());
        String alertGroupValue = templateRenderer.render(new HashMap<>(value.getMetricEvent().getTags()));
        keyBuilder.append(alertGroupValue);
        return keyBuilder.toString();
    }
}
