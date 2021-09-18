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
import cloud.erda.analyzer.tracing.model.Span;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/9/17 22:12
 */
public class SpanMetricCompatibleMapper implements MapFunction<Span, MetricEvent> {

    @Override
    public MetricEvent map(Span span) throws Exception {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setName(SpanConstants.SPAN_METRIC_NAME);
        metricEvent.setTimestamp(span.getStartTimeUnixNano());
        Map<String, String> tags = metricEvent.getTags();
        tags.put(SpanConstants.TRACE_ID, span.getTraceID());
        tags.put(SpanConstants.SPAN_ID, span.getSpanID());
        tags.put(SpanConstants.PARENT_SPAN_ID, span.getParentSpanID());
        tags.put(SpanConstants.OPERATION_NAME, span.getName());
        tags.putAll(span.getAttributes());
        Map<String, Object> fields = metricEvent.getFields();
        fields.put(SpanConstants.START_TIME, span.getStartTimeUnixNano());
        fields.put(SpanConstants.END_TIME, span.getEndTimeUnixNano());
        return metricEvent;
    }
}
