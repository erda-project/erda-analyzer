/*
 * Copyright (c) 2021 Terminus, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.erda.analyzer.tracing.functions;

import cloud.erda.analyzer.common.constant.SpanConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.tracing.model.Span;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/9/18 14:06
 */
@Slf4j
public class TransactionAnalysisFunction extends ProcessWindowFunction<Span, MetricEvent, String, TimeWindow> {

    @Override
    public void process(String s, ProcessWindowFunction<Span, MetricEvent, String, TimeWindow>.Context context, Iterable<Span> iterable, Collector<MetricEvent> collector) throws Exception {
        Map<String, Span> spans = new HashMap<>();
        for (Span span : iterable) {
            spans.put(span.getSpanID(), span);
        }
        for (Map.Entry<String, Span> entry : spans.entrySet()) {
            Span span = entry.getValue();
            String layer = getAttribute(span, SpanConstants.SPAN_LAYER);
            String kind = getAttribute(span, SpanConstants.SPAN_KIND);

            MetricEvent metricEvent = null;

            switch (layer) {
                case SpanConstants.SPAN_LAYER_HTTP:
                    if (SpanConstants.SPAN_KIND_SERVER.equals(kind)) {
                        metricEvent = createServerMetrics(spans, span, SpanConstants.APPLICATION_HTTP);
                    }
                    if (SpanConstants.SPAN_KIND_CLIENT.equals(kind)) {
                        metricEvent = createClientMetrics(span, SpanConstants.APPLICATION_HTTP);
                    }
                    break;
                case SpanConstants.SPAN_LAYER_RPC:
                    if (SpanConstants.SPAN_KIND_SERVER.equals(kind)) {
                        metricEvent = createServerMetrics(spans, span, SpanConstants.APPLICATION_RPC);
                    }
                    if (SpanConstants.SPAN_KIND_CLIENT.equals(kind)) {
                        metricEvent = createClientMetrics(span, SpanConstants.APPLICATION_RPC);
                    }
                    break;
                case SpanConstants.SPAN_LAYER_CACHE:
                    metricEvent = createClientMetrics(span, SpanConstants.SPAN_LAYER_CACHE);
                    break;
                case SpanConstants.SPAN_LAYER_DB:
                    metricEvent = createClientMetrics(span, SpanConstants.APPLICATION_DB);
                    break;
                case SpanConstants.SPAN_LAYER_MQ:
                    if (SpanConstants.SPAN_KIND_PRODUCER.equals(kind)) {
                        metricEvent = createClientMetrics(span, SpanConstants.APPLICATION_MQ);
                    }
                    if (SpanConstants.SPAN_KIND_CONSUMER.equals(kind)) {
                        metricEvent = createServerMetrics(spans, span, SpanConstants.APPLICATION_MQ);
                    }
                    break;
            }
            if (metricEvent != null) {
                metricEvent.setTimestamp(span.getEndTimeUnixNano());
                metricEvent.addField(SpanConstants.ELAPSED, span.getEndTimeUnixNano() - span.getStartTimeUnixNano());
                metricEvent.getTags().putAll(span.getAttributes());
                collector.collect(metricEvent);
            }
        }
    }

    private MetricEvent createServerMetrics(Map<String, Span> spans, Span span, String metricName) {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setName(metricName);
        metricEvent.addTag(SpanConstants.TARGET_SERVICE_ID, getAttribute(span, SpanConstants.SERVICE_ID));
        metricEvent.addTag(SpanConstants.TARGET_SERVICE_NAME, getAttribute(span, SpanConstants.SERVICE_NAME));
        metricEvent.addTag(SpanConstants.TARGET_MSP_ENV_ID, getAttribute(span, SpanConstants.MSP_ENV_ID));
        metricEvent.addTag(SpanConstants.TARGET_TERMINUS_KEY, getAttribute(span, SpanConstants.TERMINUS_KEY));
        metricEvent.addTag(SpanConstants.TARGET_SERVICE_INSTANCE_ID, getAttribute(span, SpanConstants.SERVICE_INSTANCE_ID));
        String parentSpanId = span.getParentSpanID();
        if (StringUtil.isNotEmpty(parentSpanId)) {
            Span parentSpan = spans.get(parentSpanId);
            if (parentSpan != null) {
                metricEvent.addTag(SpanConstants.SOURCE_SERVICE_ID, getAttribute(parentSpan, SpanConstants.SERVICE_ID));
                metricEvent.addTag(SpanConstants.SOURCE_SERVICE_NAME, getAttribute(parentSpan, SpanConstants.SERVICE_NAME));
                metricEvent.addTag(SpanConstants.SOURCE_TERMINUS_KEY, getAttribute(parentSpan, SpanConstants.MSP_ENV_ID));
                metricEvent.addTag(SpanConstants.SOURCE_MSP_ENV_ID, getAttribute(parentSpan, SpanConstants.TERMINUS_KEY));
                metricEvent.addTag(SpanConstants.SOURCE_SERVICE_INSTANCE_ID, getAttribute(parentSpan, SpanConstants.SERVICE_INSTANCE_ID));
            }
        }
        try {
            String statusCode = getAttribute(span, SpanConstants.HTTP_STATUS_CODE);
            metricEvent.addField(SpanConstants.HTTP_STATUS_CODE, Long.parseLong(statusCode));
        } catch (Exception ignored) {
        }
        return metricEvent;
    }

    private MetricEvent createClientMetrics(Span span, String metricName) {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setName(metricName);
        metricEvent.addTag(SpanConstants.SOURCE_SERVICE_ID, getAttribute(span, SpanConstants.SERVICE_ID));
        metricEvent.addTag(SpanConstants.SOURCE_SERVICE_NAME, getAttribute(span, SpanConstants.SERVICE_NAME));
        metricEvent.addTag(SpanConstants.SOURCE_TERMINUS_KEY, getAttribute(span, SpanConstants.MSP_ENV_ID));
        metricEvent.addTag(SpanConstants.SOURCE_MSP_ENV_ID, getAttribute(span, SpanConstants.TERMINUS_KEY));
        metricEvent.addTag(SpanConstants.SOURCE_SERVICE_INSTANCE_ID, getAttribute(span, SpanConstants.SERVICE_INSTANCE_ID));
        return metricEvent;
    }

    private String getAttribute(Span span, String key) {
        return span.getAttributes().get(key);
    }
}
