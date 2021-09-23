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

/**
 * @author liuhaoyang
 * @date 2021/9/21 00:59
 */
public class SpanToServiceMetricFunction implements MapFunction<Span, MetricEvent> {

    @Override
    public MetricEvent map(Span span) throws Exception {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setTimestamp(span.getEndTimeUnixNano());
        metricEvent.setName(SpanConstants.APPLICATION_SERVICE_NODE);
        metricEvent.addTag(SpanConstants.MSP_ENV_ID, span.getAttributes().get(SpanConstants.MSP_ENV_ID));
        metricEvent.addTag(SpanConstants.TERMINUS_KEY, span.getAttributes().get(SpanConstants.MSP_ENV_ID));
        metricEvent.addTag(SpanConstants.SERVICE_ID, span.getAttributes().get(SpanConstants.SERVICE_ID));
        metricEvent.addTag(SpanConstants.SERVICE_NAME, span.getAttributes().get(SpanConstants.SERVICE_NAME));
        metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_ID, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_ID));
//        if (span.getAttributes().containsKey(SpanConstants.SERVICE_INSTANCE_ID)) {
//            metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_ID, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_ID));
//        } else {
//            metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_ID, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_IP));
//        }
        metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_IP, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_IP));
        metricEvent.addTag(SpanConstants.PROJECT_NAME, span.getAttributes().get(SpanConstants.PROJECT_NAME));
        metricEvent.addTag(SpanConstants.WORKSPACE, span.getAttributes().get(SpanConstants.WORKSPACE));
        if (span.getAttributes().containsKey(SpanConstants.JAEGER_VERSION)) {
            metricEvent.addTag(SpanConstants.INSTRUMENTATION_LIBRARY, SpanConstants.JAEGER);
            metricEvent.addTag(SpanConstants.INSTRUMENTATION_LIBRARY_VERSION, span.getAttributes().get(SpanConstants.JAEGER_VERSION));
        }
        int startTimeCount = 0;
        Long startAt = getServiceInstanceStartedAt(span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_STARTED_AT));
        if (startAt != null) {
            startTimeCount++;
            metricEvent.addField(SpanConstants.START_TIME_MEAN, startAt);
        }
        metricEvent.addField(SpanConstants.START_TIME_COUNT, startTimeCount);
        return metricEvent;
    }

    private Long getServiceInstanceStartedAt(String start) {
        if (start == null) {
            return null;
        }
        try {
            return Long.parseLong(start);
        } catch (Exception exception) {
            return null;
        }
    }
}
