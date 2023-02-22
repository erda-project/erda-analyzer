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
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.tracing.model.Span;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author liuhaoyang
 * @date 2021/9/18 14:06
 */
@Slf4j
public class TraceAnalysisFunction extends ProcessWindowFunction<Span, MetricEvent, String, TimeWindow> {

    @Override
    public void process(String s, ProcessWindowFunction<Span, MetricEvent, String, TimeWindow>.Context context, Iterable<Span> iterable, Collector<MetricEvent> collector) throws Exception {
        Map<String, Span> spans = new HashMap<>();
        Map<String, Span> services = new HashMap<>();
        Set<String> parentIds = new HashSet<>();
        for (Span span : iterable) {
            spans.put(span.getSpanID(), span);
            if (StringUtil.isNotEmpty(span.getParentSpanID())) {
                parentIds.add(span.getParentSpanID());
            }
            String serviceKey = getAttribute(span, SpanConstants.ENV_ID) + getAttribute(span, SpanConstants.TERMINUS_KEY) + getAttribute(span, SpanConstants.SERVICE_ID) + getAttribute(span, SpanConstants.SERVICE_INSTANCE_ID);
            Span lastServiceSpan = services.get(serviceKey);
            if (lastServiceSpan == null || span.getEndTimeUnixNano() > lastServiceSpan.getEndTimeUnixNano()) {
                services.put(serviceKey, span);
            }
        }

        // process service node
        for (Span span : services.values()) {
            MetricEvent metricEvent = createServiceMetrics(span, SpanConstants.APPLICATION_SERVICE_NODE);
            int startTimeCount = 0;
            metricEvent.addField(SpanConstants.START_TIME, startTimeCount);
            if (log.isDebugEnabled()) {
                log.debug("Map span to service metric @SpanEndTime {}. {} ", new Date(metricEvent.getTimestamp() / 1000000), JsonMapperUtils.toStrings(metricEvent));
            }
            collector.collect(metricEvent);
        }

        // process http&rpc&db&cache&mq call metrics
        for (Span span : iterable) {
            String layer = getAttribute(span, SpanConstants.SPAN_LAYER);
            String kind = getAttribute(span, SpanConstants.SPAN_KIND);

            MetricEvent metricEvent = null;

            switch (layer) {
                case SpanConstants.SPAN_LAYER_HTTP:
                    if (SpanConstants.SPAN_KIND_SERVER.equals(kind)) {
                        metricEvent = createServerMetrics(spans, span, SpanConstants.APPLICATION_HTTP);
                    }
                    if (SpanConstants.SPAN_KIND_CLIENT.equals(kind)) {
                        metricEvent = createClientMetrics(parentIds, span, SpanConstants.APPLICATION_HTTP);
                    }
                    break;
                case SpanConstants.SPAN_LAYER_RPC:
                    if (SpanConstants.SPAN_KIND_SERVER.equals(kind)) {
                        metricEvent = createServerMetrics(spans, span, SpanConstants.APPLICATION_RPC);
                    }
                    if (SpanConstants.SPAN_KIND_CLIENT.equals(kind)) {
                        metricEvent = createClientMetrics(parentIds, span, SpanConstants.APPLICATION_RPC);
                    }
                    break;
                case SpanConstants.SPAN_LAYER_CACHE:
                    metricEvent = createClientMetrics(parentIds, span, SpanConstants.APPLICATION_CACHE);
                    break;
                case SpanConstants.SPAN_LAYER_DB:
                    metricEvent = createClientMetrics(parentIds, span, SpanConstants.APPLICATION_DB);
                    break;
                case SpanConstants.SPAN_LAYER_MQ:
                    if (SpanConstants.SPAN_KIND_PRODUCER.equals(kind)) {
                        metricEvent = createClientMetrics(parentIds, span, SpanConstants.APPLICATION_MQ);
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
                metricEvent.addTag(SpanConstants.TRACE_ID, span.getTraceID());
                metricEvent.addTag(SpanConstants.REQUEST_ID, span.getTraceID());
                metricEvent.addTag(SpanConstants.TRACE_SAMPLED, SpanConstants.TRUE);
                if (log.isDebugEnabled()) {
                    log.debug("Map span to transaction metric @SpanEndTime {}. {}", new Date(metricEvent.getTimestamp() / 1000000), JsonMapperUtils.toStrings(metricEvent));
                }
                collector.collect(metricEvent);
            }
        }
    }

    private MetricEvent createServerMetrics(Map<String, Span> traceSpans, Span span, String metricName) {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setName(metricName);
        metricEvent.addTag(SpanConstants.TARGET_SERVICE_ID, getAttribute(span, SpanConstants.SERVICE_ID));
        metricEvent.addTag(SpanConstants.TARGET_SERVICE_NAME, getAttribute(span, SpanConstants.SERVICE_NAME));
        metricEvent.addTag(SpanConstants.TARGET_ENV_ID, getAttribute(span, SpanConstants.ENV_ID));
        metricEvent.addTag(SpanConstants.TARGET_TERMINUS_KEY, getAttribute(span, SpanConstants.TERMINUS_KEY));
        metricEvent.addTag(SpanConstants.TARGET_SERVICE_INSTANCE_ID, getAttribute(span, SpanConstants.SERVICE_INSTANCE_ID));
        String parentSpanId = span.getParentSpanID();
        if (StringUtil.isNotEmpty(parentSpanId)) {
            Span parentSpan = traceSpans.get(parentSpanId);
            if (parentSpan != null) {
                metricEvent.addTag(SpanConstants.SOURCE_SERVICE_ID, getAttribute(parentSpan, SpanConstants.SERVICE_ID));
                metricEvent.addTag(SpanConstants.SOURCE_SERVICE_NAME, getAttribute(parentSpan, SpanConstants.SERVICE_NAME));
                metricEvent.addTag(SpanConstants.SOURCE_TERMINUS_KEY, getAttribute(parentSpan, SpanConstants.ENV_ID));
                metricEvent.addTag(SpanConstants.SOURCE_ENV_ID, getAttribute(parentSpan, SpanConstants.TERMINUS_KEY));
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

    private MetricEvent createClientMetrics(Set<String> parentIds, Span span, String metricName) {
        // If the client span has child spans, this span will is ignored
        if (parentIds.contains(span.getSpanID())) {
            return null;
        }
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setName(metricName);
        metricEvent.addTag(SpanConstants.SOURCE_SERVICE_ID, getAttribute(span, SpanConstants.SERVICE_ID));
        metricEvent.addTag(SpanConstants.SOURCE_SERVICE_NAME, getAttribute(span, SpanConstants.SERVICE_NAME));
        metricEvent.addTag(SpanConstants.SOURCE_TERMINUS_KEY, getAttribute(span, SpanConstants.ENV_ID));
        metricEvent.addTag(SpanConstants.SOURCE_ENV_ID, getAttribute(span, SpanConstants.TERMINUS_KEY));
        metricEvent.addTag(SpanConstants.SOURCE_SERVICE_INSTANCE_ID, getAttribute(span, SpanConstants.SERVICE_INSTANCE_ID));
        return metricEvent;
    }

    private MetricEvent createServiceMetrics(Span span, String metricName) {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setTimestamp(span.getEndTimeUnixNano());
        metricEvent.setName(metricName);
        metricEvent.addTag(SpanConstants.ENV_ID, span.getAttributes().get(SpanConstants.ENV_ID));
        metricEvent.addTag(SpanConstants.TERMINUS_KEY, span.getAttributes().get(SpanConstants.ENV_ID));
        metricEvent.addTag(SpanConstants.SERVICE_ID, span.getAttributes().get(SpanConstants.SERVICE_ID));
        metricEvent.addTag(SpanConstants.SERVICE_NAME, span.getAttributes().get(SpanConstants.SERVICE_NAME));
        metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_ID, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_ID));
        metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_IP, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_IP));
        metricEvent.addTag(SpanConstants.PROJECT_NAME, span.getAttributes().get(SpanConstants.PROJECT_NAME));
        metricEvent.addTag(SpanConstants.WORKSPACE, span.getAttributes().get(SpanConstants.WORKSPACE));
        metricEvent.addTag(SpanConstants.HOST_IP, span.getAttributes().get(SpanConstants.HOST_IP));
        metricEvent.addTag(SpanConstants.SERVICE_IP, span.getAttributes().get(SpanConstants.SERVICE_IP));
        return metricEvent;
    }

    private String getAttribute(Span span, String key) {
        return span.getAttributes().get(key);
    }
}
