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

import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: liuhaoyang
 * @create: 2020-01-08 11:50
 **/
public class MetricFormatProcessFunction implements MapFunction<AggregatedMetricEvent, AggregatedMetricEvent> {

    @Override
    public AggregatedMetricEvent map(AggregatedMetricEvent value) throws Exception {
//        for (AggregateResult result : value.getResults()) {
//            FunctionFormatter formatter = FunctionFormatterFactory.create(result.getFunction().getFormatter());
//            if (formatter != null) {
//                Object formatted = formatter.format(result.getValue());
//                result.setFormattedValue(formatted);
//            }
//        }
        return value;
    }
}
