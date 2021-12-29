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
import cloud.erda.analyzer.tracing.model.Span;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;

/**
 * @author liuhaoyang
 * @date 2021/9/21 00:59
 */
@Slf4j
public class SpanToServiceMetricFunction implements MapFunction<Span, MetricEvent> {

    @Override
    public MetricEvent map(Span span) throws Exception {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setTimestamp(span.getEndTimeUnixNano());
        metricEvent.setName(SpanConstants.APPLICATION_SERVICE_NODE);
        metricEvent.addTag(SpanConstants.ENV_ID, span.getAttributes().get(SpanConstants.ENV_ID));
        metricEvent.addTag(SpanConstants.TERMINUS_KEY, span.getAttributes().get(SpanConstants.ENV_ID));
        metricEvent.addTag(SpanConstants.SERVICE_ID, span.getAttributes().get(SpanConstants.SERVICE_ID));
        metricEvent.addTag(SpanConstants.SERVICE_NAME, span.getAttributes().get(SpanConstants.SERVICE_NAME));
        metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_ID, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_ID));
        metricEvent.addTag(SpanConstants.SERVICE_INSTANCE_IP, span.getAttributes().get(SpanConstants.SERVICE_INSTANCE_IP));
        metricEvent.addTag(SpanConstants.PROJECT_NAME, span.getAttributes().get(SpanConstants.PROJECT_NAME));
        metricEvent.addTag(SpanConstants.WORKSPACE, span.getAttributes().get(SpanConstants.WORKSPACE));
        int startTimeCount = 0;
        metricEvent.addField(SpanConstants.START_TIME_COUNT, startTimeCount);
        if (log.isDebugEnabled()) {
            log.debug("Map reduced span to service metric @SpanEndTime {}. {} ", new Date(metricEvent.getTimestamp() / 1000000), JsonMapperUtils.toStrings(metricEvent));
        }
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
