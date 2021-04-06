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

package cloud.erda.analyzer.metrics.functions;

import cloud.erda.analyzer.common.constant.MetricConstants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2020/9/25 22:15
 */
public class MachineMetricsPreProcessFunction extends ProcessWindowFunction<MetricEvent, MetricEvent, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<MetricEvent> iterable, Collector<MetricEvent> collector) throws Exception {
        MetricEvent statusEvent = new MetricEvent();
        statusEvent.setName(MetricConstants.PRE_MACHINE_STATUS);
        statusEvent.setTimestamp(context.currentWatermark() * 1000 * 1000);
        Map<String, String> tags = statusEvent.getTags();
        int count = 0;
        for (MetricEvent item : iterable) {
            tags.put(MetricTagConstants.ORG_NAME, item.getTags().get(MetricTagConstants.ORG_NAME));
            tags.put(MetricTagConstants.CLUSTER_NAME, item.getTags().get(MetricTagConstants.CLUSTER_NAME));
            tags.put(MetricTagConstants.HOSTIP, item.getTags().get(MetricTagConstants.HOSTIP));
            tags.put(MetricTagConstants.HOST, item.getTags().get(MetricTagConstants.HOST));
            if (item.getTags().containsKey(MetricTagConstants.LABELS)) {
                tags.put(MetricTagConstants.LABELS, item.getTags().get(MetricTagConstants.LABELS));
            }
            count++;
        }
        statusEvent.getFields().put(MetricConstants.COUNT, count);
        collector.collect(statusEvent);
    }
}
