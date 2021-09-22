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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author liuhaoyang
 * @date 2021/9/22 13:27
 */
public class MetricMetaFunction implements FlatMapFunction<MetricEvent, MetricEvent> {
    @Override
    public void flatMap(MetricEvent metricEvent, Collector<MetricEvent> collector) throws Exception {

        metricEvent.addTag(SpanConstants.META, SpanConstants.META_TRUE);
        metricEvent.addTag(SpanConstants.METRIC_SCOPE, SpanConstants.METRIC_SCOPE_MICRO_SERVICE);
        metricEvent.addTag(SpanConstants.METRIC_SCOPE_ID, metricEvent.getTags().get(SpanConstants.MSP_ENV_ID));
        collector.collect(metricEvent);
    }
}
