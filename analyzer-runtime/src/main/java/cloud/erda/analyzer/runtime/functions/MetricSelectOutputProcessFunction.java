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

import cloud.erda.analyzer.common.constant.ExpressionConstants;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liuhaoyang
 * @date 2021/1/28 15:15
 */
public class MetricSelectOutputProcessFunction extends ProcessFunction<AggregatedMetricEvent, AggregatedMetricEvent> {

    private OutputTag<AggregatedMetricEvent> metricTag;

    private OutputTag<AggregatedMetricEvent> metricTempTag;

    private OutputTag<AggregatedMetricEvent> alertEventTag;

    public MetricSelectOutputProcessFunction(OutputTag<AggregatedMetricEvent> metricTag, OutputTag<AggregatedMetricEvent> metricTempTag, OutputTag<AggregatedMetricEvent> alertEventTag) {
        this.metricTag = metricTag;
        this.metricTempTag = metricTempTag;
        this.alertEventTag = alertEventTag;
    }

    @Override
    public void processElement(AggregatedMetricEvent element, Context context, Collector<AggregatedMetricEvent> collector) throws Exception {
        if (element.getOutputs().contains(ExpressionConstants.OUTPUT_METRIC) && metricTag != null) {
            context.output(metricTag, element);
        }
        if (element.getOutputs().contains(ExpressionConstants.OUTPUT_TEMP_METRIC) && metricTempTag != null) {
            context.output(metricTempTag, element);
        }
        if (element.getOutputs().contains(ExpressionConstants.OUTPUT_ALERT) && alertEventTag != null) {
            context.output(alertEventTag, element);
        }
    }
}
