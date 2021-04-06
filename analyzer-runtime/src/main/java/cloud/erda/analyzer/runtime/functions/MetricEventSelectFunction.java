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

package cloud.erda.analyzer.runtime.functions;

import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.OutputMetricEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author: liuhaoyang
 * @create: 2019-09-17 22:04
 **/
public class MetricEventSelectFunction implements MetricSelectProcessFunction, FlatMapFunction<AggregatedMetricEvent, MetricEvent> {

    @Override
    public MetricEvent mapOutputMetricEvent(OutputMetricEvent value) throws Exception {
        MetricEvent result = new MetricEvent();
        result.setName(value.getAlias());
        result.setTimestamp(value.getTimestamp());
        result.getTags().putAll(value.getAggregatedTags());
        result.getFields().putAll(value.getAggregatedFields());
        return result;
    }

    @Override
    public void flatMap(AggregatedMetricEvent value, Collector<MetricEvent> out) throws Exception {
        if (value.isOperatorResult()) {
            out.collect(mapAggregatedMetricEvent(value));
        }
    }
}
