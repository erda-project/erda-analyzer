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

import cloud.erda.analyzer.alert.models.NotifyEvent;
import cloud.erda.analyzer.alert.models.RenderedNotifyEvent;
import cloud.erda.analyzer.alert.templates.TemplateContext;
import cloud.erda.analyzer.alert.templates.TemplateManager;
import cloud.erda.analyzer.alert.templates.TemplateRenderer;
import cloud.erda.analyzer.alert.templates.formatters.TemplateFormatter;
import cloud.erda.analyzer.alert.templates.formatters.TemplateFormatterFactory;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.utils.DateUtils;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

import static cloud.erda.analyzer.common.utils.DateUtils.YYYY_MM_DD_HH_MM_SS;

public class NotifyEventTemplateRenderFunction implements MapFunction<NotifyEvent, RenderedNotifyEvent> {

    private TemplateManager templateManager = new TemplateManager();

    @Override
    public RenderedNotifyEvent map(NotifyEvent notifyEvent) throws Exception {
        String renderId = getRenderedNotifyEventId(notifyEvent);
        RenderedNotifyEvent result = new RenderedNotifyEvent();
        result.setId(renderId);
        result.setMetricEvent(notifyEvent.getMetricEvent());
        result.setNotifyTarget(notifyEvent.getNotify().getTarget());
        result.setNotifyName(notifyEvent.getNotify().getNotifyName());
        result.setNotify(notifyEvent.getNotify());
        result.setTemplateTarget(notifyEvent.getNotifyTemplate().getTemplate().getTarget());
        //将fields转换为map
        Map<String, Object> templateContext = TemplateContext.fromMetric(result.getMetricEvent());
        Map<String, Object> attribute = JsonMapperUtils.toObjectValueMap(notifyEvent.getNotify().getAttribute());
        if (attribute != null) {
            templateContext.putAll(attribute);
        }
        long timestamp = notifyEvent.getMetricEvent().getTimestamp() / (1000 * 1000);
        templateContext.put(AlertConstants.TIMESTAMP, timestamp);
        templateContext.put(AlertConstants.TIMESTAMP_UNIX, timestamp);

        templateContext.put(AlertConstants.TIMESTAMP, DateUtils.format(timestamp, YYYY_MM_DD_HH_MM_SS));
        Map<String, String> formats = notifyEvent.getNotifyTemplate().getTemplate().getRender().getFormats();
        if (formats != null) {
            templateContext = processFormatter(templateContext, notifyEvent.getNotifyTemplate().getTemplate().getRender().getFormats());
        }
        TemplateRenderer titleRenderer = templateManager.getRenderer(notifyEvent.getNotifyTemplate().getNotifyId() + "_title", notifyEvent.getNotifyTemplate().getTemplate().getRender().getTitle(), false);
        result.setTitle(titleRenderer.render(templateContext));

        TemplateRenderer contentRenderer = templateManager.getRenderer(notifyEvent.getNotifyTemplate().getNotifyId() + "_content", notifyEvent.getNotifyTemplate().getTemplate().getRender().getTemplate(), false);
        result.setContent(contentRenderer.render(templateContext));
        return result;
    }

    private Map<String, Object> processFormatter(Map<String, Object> templateContext, Map<String, String> formatters) {
        Map<String, Object> newTemplateContext = new HashMap<>(templateContext);
        for (Map.Entry<String, Object> item : templateContext.entrySet()) {
            if (item.getValue() == null) {
                continue;
            }
            String formatter = formatters.get(item.getKey());
            TemplateFormatter templateFormatter = TemplateFormatterFactory.create(formatter);
            if (templateFormatter == null) {
                if (!(item.getValue() instanceof Number)) {
                    continue;
                }
            }
            Object formattedValue = templateFormatter.format(item.getValue());
            if (formattedValue == null) {
                continue;
            }
            newTemplateContext.put(item.getKey(), formattedValue);
        }
        return newTemplateContext;
    }

    private String getRenderedNotifyEventId(NotifyEvent value) {
        Long notifyId = value.getNotifyId();
        String notifyEventId = notifyId + "-" + value.getTemplateId();
        return notifyEventId;
    }
}
