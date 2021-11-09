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

package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertTrigger;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.runtime.expression.functions.aggregators.Accumulator;
import cloud.erda.analyzer.runtime.expression.functions.aggregators.DefaultAccumulator;
import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static cloud.erda.analyzer.common.constant.MetricTagConstants.TRIGGER_DURATION;

/**
 * @author: liuhaoyang
 * @create: 2020-01-05 23:13
 **/

public class AlertEventLevelReduceFunction implements ReduceFunction<AlertEvent> {

    @Override
    public AlertEvent reduce(AlertEvent first, AlertEvent second) throws Exception {
        AlertLevel level1 = AlertLevel.valueOf(first.getMetricEvent().getTags().get("level"));
        AlertLevel level2 = AlertLevel.valueOf(second.getMetricEvent().getTags().get("level"));
        AlertEvent event = level1.compareTo(level2) > 0 ? first : second;
        System.out.println("xxxxxxxxxxthe reduce max level is "+event);
        return event;
    }

    public enum AlertLevel {
        Breakdown,
        Emergency,
        Alert,
        Light
    }
}
