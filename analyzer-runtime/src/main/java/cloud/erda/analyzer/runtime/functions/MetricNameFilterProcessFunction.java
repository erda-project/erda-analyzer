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
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.util.Collector;

import static cloud.erda.analyzer.common.constant.Constants.EMPTY_STRING;
import static cloud.erda.analyzer.common.utils.StringUtil.isNotEmpty;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 11:36
 **/
public class MetricNameFilterProcessFunction extends ExpressionBroadcastProcessFunction<MetricEvent, MetricEvent> {

    private MapStateDescriptor<String, String> metricStateDescriptor;

    public MetricNameFilterProcessFunction(MapStateDescriptor<String, String> metricState) {
        metricStateDescriptor = metricState;
    }

    @Override
    public void processElement(MetricEvent value, ReadOnlyContext ctx, Collector<MetricEvent> out) throws Exception {
        /**
         * 根据alert_key和metric_name进行第一次filter
         * 使用metric_name过滤，减少流入到真正计算节点的数据量
         */
        if (filterMetricName(value, ctx)) {
            out.collect(value);
        }
    }

    @Override
    public void processBroadcastElement(ExpressionMetadata value, Context ctx, Collector<MetricEvent> out) throws Exception {
        if (value != null) {
            for (String metric : value.getExpression().getMetrics()) {
                ctx.getBroadcastState(metricStateDescriptor).put(metric, EMPTY_STRING);
            }
        }
    }

    private boolean filterMetricName(MetricEvent value, ReadOnlyContext ctx) throws Exception {
        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(metricStateDescriptor);
        return isNotEmpty(value.getName()) && state.contains(value.getName());
    }
}
