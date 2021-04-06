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

import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import cloud.erda.analyzer.runtime.models.WindowBehavior;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liuhaoyang
 * @date 2021/1/28 13:05
 */
public class WindowBehaviorOutputProcessFunction extends ProcessFunction<KeyedMetricEvent, KeyedMetricEvent> {

    private final OutputTag<KeyedMetricEvent> allowedLatenessTag;

    public WindowBehaviorOutputProcessFunction(OutputTag<KeyedMetricEvent> allowedLatenessTag) {
        this.allowedLatenessTag = allowedLatenessTag;
    }

    @Override
    public void processElement(KeyedMetricEvent element, Context context, Collector<KeyedMetricEvent> collect) throws Exception {
        if (element.getExpression().getWindowBehavior().equals(WindowBehavior.allowedLateness)) {
            context.output(allowedLatenessTag, element);
        } else {
            collect.collect(element);
        }
    }
}
