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

import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/9/23 22:49
 */
@Slf4j
public class StatsAccumulator implements Serializable {

    private MetricEvent lastMetric;

    private Map<String, FieldAggregator> aggregators = new HashMap<>();

    public void apply(MetricEvent metricEvent) {
        if (metricEvent == null) {
            return;
        }
        lastMetric = metricEvent;
        for (Map.Entry<String, Object> entry : metricEvent.getFields().entrySet()) {
            FieldAggregator aggregator = aggregators.computeIfAbsent(entry.getKey(), FieldAggregator::new);
            aggregator.apply(entry.getValue());
        }
    }

    public MetricEvent getResult() throws IOException {
        if (lastMetric == null) {
            return null;
        }
        MetricEvent metricEvent = lastMetric.copy();
        metricEvent.getFields().clear();
        for (Map.Entry<String, FieldAggregator> entry : aggregators.entrySet()) {
            FieldAggregator aggregator = entry.getValue();
            metricEvent.addField(aggregator.getName() + "_mean", aggregator.getMean());
            metricEvent.addField(aggregator.getName() + "_count", aggregator.getCount());
            metricEvent.addField(aggregator.getName() + "_sum", aggregator.getSum());
            metricEvent.addField(aggregator.getName() + "_min", aggregator.getMin());
            metricEvent.addField(aggregator.getName() + "_max", aggregator.getMax());
        }
        if (log.isDebugEnabled()) {
            log.debug("transaction metric aggregate @SpanEndTime {} . {}", new Date(metricEvent.getTimestamp() / 1000000), JsonMapperUtils.toStrings(metricEvent));
        }
        return metricEvent;
    }
}

