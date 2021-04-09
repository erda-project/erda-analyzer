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

package cloud.erda.analyzer.common.functions;

import cloud.erda.analyzer.common.models.MetricEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author liuhaoyang
 * @date 2020/9/25 17:23
 */
public class MetricEventCorrectFunction implements FlatMapFunction<MetricEvent, MetricEvent> {

//    @Override
//    public boolean filter(MetricEvent metricEvent) throws Exception {
//        return metricEvent != null;
//    }

    @Override
    public void flatMap(MetricEvent metricEvent, Collector<MetricEvent> collector) throws Exception {
        if (metricEvent != null) {
            long currentTimestamp = System.currentTimeMillis() * 1000 * 1000;
            if (metricEvent.getTimestamp() > currentTimestamp) {
                metricEvent.setTimestamp(currentTimestamp);
            }
            collector.collect(metricEvent);
        }
    }
}
