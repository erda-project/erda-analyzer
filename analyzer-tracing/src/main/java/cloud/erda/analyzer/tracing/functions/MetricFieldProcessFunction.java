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
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author liuhaoyang
 * @date 2021/9/27 11:30
 */
@Slf4j
public class MetricFieldProcessFunction extends ProcessWindowFunction<MetricEvent, MetricEvent, String, TimeWindow> {

    @Override
    public void process(String s, ProcessWindowFunction<MetricEvent, MetricEvent, String, TimeWindow>.Context context, Iterable<MetricEvent> iterable, Collector<MetricEvent> collector) throws Exception {
        log.info("current key = {}", s);
        StatsAccumulator statsAccumulator = new StatsAccumulator();
        for (MetricEvent metricEvent : iterable) {
            statsAccumulator.apply(metricEvent);
        }
        MetricEvent result = statsAccumulator.getResult();
        if (result != null) {
            collector.collect(result);
        }
    }
}
