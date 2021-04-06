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

import cloud.erda.analyzer.alert.models.RenderedAlertEvent;
import cloud.erda.analyzer.alert.models.Ticket;
import cloud.erda.analyzer.alert.templates.TemplateManager;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.MetricConstants;
import cloud.erda.analyzer.alert.templates.TemplateRenderer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-01-07 15:43
 **/
public class AlertTargetToTicketMapFunction implements MapFunction<RenderedAlertEvent, Ticket> {

    private TemplateManager templateManager = new TemplateManager();

    @Override
    public Ticket map(RenderedAlertEvent value) throws Exception {
        Map<String, String> metricTags = value.getMetricEvent().getTags();

        Ticket ticket = new Ticket();
        ticket.setKey(DigestUtils.md5Hex(metricTags.get(AlertConstants.ALERT_GROUP_ID)));
        ticket.setPriority(MetricConstants.MEDIUM);
        ticket.setTitle(value.getTitle());
        ticket.setContent(value.getContent());
        ticket.setOrgId(metricTags.getOrDefault(AlertConstants.DICE_ORG_ID, AlertConstants.INVALID_ORG_ID));
        ticket.setTargetType(metricTags.get(AlertConstants.ALERT_SCOPE));
        ticket.setTargetID(metricTags.get(AlertConstants.ALERT_SCOPE_ID));
        ticket.setType(metricTags.get(AlertConstants.ALERT_TYPE));

        ticket.setMetric(metricTags.get(AlertConstants.ALERT_INDEX));
        String metricIdKey = metricTags.get(AlertConstants.TICKETS_METRIC_KEY);

        TemplateRenderer templateRenderer = templateManager.getRenderer(AlertConstants.TICKETS_METRIC_KEY + metricIdKey, metricIdKey, false);

        String displayUrl = metricTags.get(AlertConstants.DISPLAY_URL);
        if (displayUrl != null) {
            metricTags.put(AlertConstants.PATH, displayUrl);
        }

        ticket.setLabel(metricTags);
        ticket.setMetricID(templateRenderer.render(new HashMap<>(metricTags)));
        ticket.setTriggeredAt(value.getMetricEvent().getTimestamp() / (1000 * 1000));
        return ticket;
    }
}
