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

import cloud.erda.analyzer.alert.templates.TemplateContext;
import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.RenderedAlertEvent;
import cloud.erda.analyzer.alert.templates.TemplateManager;
import cloud.erda.analyzer.alert.templates.TemplateRenderer;
import cloud.erda.analyzer.alert.templates.formatters.FractionTemplateFormatter;
import cloud.erda.analyzer.alert.templates.formatters.TemplateFormatter;
import cloud.erda.analyzer.alert.templates.formatters.TemplateFormatterFactory;
import cloud.erda.analyzer.alert.utils.ChangeUrl;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static cloud.erda.analyzer.common.utils.DateUtils.YYYY_MM_DD_HH_MM_SS;

/**
 * @author: liuhaoyang
 * @create: 2020-01-06 01:32
 **/
@Slf4j
public class AlertEventTemplateRenderFunction implements MapFunction<AlertEvent, RenderedAlertEvent> {

    private static String pattern = "(.*)-org.*";
    private static Pattern p = Pattern.compile(pattern);

    private TemplateManager templateManager = new TemplateManager();

    @Override
    public RenderedAlertEvent map(AlertEvent value) throws Exception {
        String renderId = getRenderedAlertEventId(value);
        RenderedAlertEvent result = new RenderedAlertEvent();
        result.setId(renderId);
        result.setMetricEvent(value.getMetricEvent());
        result.setNotifyTarget(value.getAlertNotify().getNotifyTarget());
        result.setTemplateTarget(value.getAlertNotifyTemplate().getTarget());
        Map<String, Object> templateContext = TemplateContext.fromMetric(value.getMetricEvent());

        long timestamp = value.getMetricEvent().getTimestamp() / (1000 * 1000);
        templateContext.put(AlertConstants.TIMESTAMP, timestamp);
        templateContext.put(AlertConstants.TIMESTAMP_UNIX, timestamp);
//        templateContext.put(AlertConstants.TRIGGER_DURATION_MIN, getTriggerDurationMin(templateContext));
        processTriggerDuration(templateContext);

        ChangeUrl.modifyMetricEvent(value.getMetricEvent());

        String displayUrl = value.getMetricEvent().getTags().get(AlertConstants.DISPLAY_URL);
        if (displayUrl != null) {
            String displayUrlId = renderId + "_display_url";
            displayUrl = this.renderUrl(displayUrlId, displayUrl, templateContext);
            templateContext.put(AlertConstants.DISPLAY_URL, displayUrl);
            value.getMetricEvent().getTags().put(AlertConstants.DISPLAY_URL, displayUrl);
        }

        String recordUrl = value.getMetricEvent().getTags().get(AlertConstants.RECORD_URL);
        if (recordUrl != null) {
            String recordUrlId = renderId + "_record_url";
            recordUrl = this.renderUrl(recordUrlId, recordUrl, templateContext);
            templateContext.put(AlertConstants.RECORD_URL, recordUrl);
            value.getMetricEvent().getTags().put(AlertConstants.RECORD_URL, recordUrl);
        }

        templateContext.put(AlertConstants.TIMESTAMP, DateUtils.format(timestamp, YYYY_MM_DD_HH_MM_SS));

        templateContext = processFormatter(templateContext, value.getAlertNotifyTemplate().getFormats());

        TemplateRenderer titleRenderer = templateManager.getRenderer(value.getAlertNotifyTemplate().getId() + "_title", value.getAlertNotifyTemplate().getTitle(), value.getAlertNotifyTemplate().isVariable());
        result.setTitle(titleRenderer.render(templateContext));

        TemplateRenderer contentRenderer = templateManager.getRenderer(value.getAlertNotifyTemplate().getId() + "_content", value.getAlertNotifyTemplate().getTemplate(), value.getAlertNotifyTemplate().isVariable());
        result.setContent(contentRenderer.render(templateContext));
        return result;
    }

    private String getRenderedAlertEventId(AlertEvent value) {
        return value.getAlertId() + "-" + value.getAlertNotify().getId() + "-" + value.getAlertNotifyTemplate().getId();
    }

    private Map<String, Object> processFormatter(Map<String, Object> templateContext, Map<String, String> formatters) {
        Map<String, Object> newTemplateContext = new HashMap<>(templateContext);
        for (Map.Entry<String, Object> item : templateContext.entrySet()) {
            if (item.getValue() == null) continue;
            String formatter = formatters.get(item.getKey());
            TemplateFormatter templateFormatter = TemplateFormatterFactory.create(formatter);
            if (templateFormatter == null) {
                if (!(item.getValue() instanceof Number)) {
                    continue;
                }
                templateFormatter = TemplateFormatterFactory.create(FractionTemplateFormatter.DEFAULT_DEFINE);
            }
            Object formattedValue = templateFormatter.format(item.getValue());
            if (formattedValue == null) {
                continue;
            }
            newTemplateContext.put(item.getKey(), formattedValue);
        }
        return newTemplateContext;
    }

    private String getTriggerDurationMin(Map<String, Object> templateContext) {
        try {
            double d = Double.parseDouble((String) templateContext.getOrDefault(AlertConstants.TRIGGER_DURATION, "0"));
            return String.format("%.2f", d / (1000 * 60));
        } catch (Exception ex) {
            return "0";
        }
    }

    private void processTriggerDuration(Map<String, Object> templateContext) {
        Object triggerDurationStr = templateContext.get(AlertConstants.TRIGGER_DURATION);
        if (triggerDurationStr instanceof String) {
            try {
                templateContext.put(AlertConstants.TRIGGER_DURATION, Double.parseDouble((String) triggerDurationStr));
            } catch (Exception ignore) {
                templateContext.put(AlertConstants.TRIGGER_DURATION, 0);
            }
        }
    }

    private String renderUrl(String renderId, String url, Map<String, Object> templateContext) {
        TemplateRenderer urlRenderer = templateManager.getRenderer(renderId + "_url", url, false);
        Map<String, Object> templateContextEncode = new HashMap<>();
        templateContext.forEach((k, v) -> {
            Object val = v;
            if (v instanceof String) {
                try {
                    val = URLEncoder.encode((String) val, "UTF-8");
                } catch (UnsupportedEncodingException ignored) {
                }
            }
            templateContextEncode.put(k, val);
        });
        return urlRenderer.render(templateContextEncode);
    }
}
