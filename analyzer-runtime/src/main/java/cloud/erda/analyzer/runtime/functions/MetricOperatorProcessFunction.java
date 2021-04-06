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

import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.runtime.expression.functions.operators.FunctionOperator;
import cloud.erda.analyzer.runtime.expression.functions.operators.FunctionOperatorFactory;
import cloud.erda.analyzer.runtime.models.AggregateResult;
import cloud.erda.analyzer.runtime.scripts.ScriptInvoker;
import cloud.erda.analyzer.runtime.scripts.ScriptInvokerManager;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import cloud.erda.analyzer.runtime.models.FunctionCondition;
import cloud.erda.analyzer.runtime.utils.AggregateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MetricOperatorProcessFunction extends ProcessWindowFunction<AggregatedMetricEvent, AggregatedMetricEvent, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<AggregatedMetricEvent> elements, Collector<AggregatedMetricEvent> out) throws Exception {
        for (AggregatedMetricEvent element : elements) {
            boolean operatorResult = processOperators(key, context, element);
            element.setOperatorResult(operatorResult);
            out.collect(element);
        }
    }

    private boolean processOperators(String s, Context context, AggregatedMetricEvent value) throws Exception {
        /**
         * 执行表达式中 function 的  operator
         * condition 处理
         * condition = and , 所有 operator 都为 true
         * condition = or , 任一 operator 为 true
         */
        if (value.getResults().isEmpty()) {
            return true;
        }
        Map<String, Object> resultFields = new HashMap<>(value.getMetric().getFields());
        for (AggregateResult result : value.getResults()) {
            resultFields.put(AggregateUtils.getAggregatedField(result.getFunction()), result.getValue());
        }
        FunctionCondition condition = FunctionCondition.and;
        for (AggregateResult result : value.getResults()) {
            invokeScript(value, result.getFunction(), resultFields);
            FunctionOperator functionOperator = FunctionOperatorFactory.create(result.getFunction());
            boolean op = functionOperator.invoke(result);
            condition = result.getFunction().getCondition();
            if (condition.equals(FunctionCondition.and) && !op) {
                return false;
            }
            if (condition.equals(FunctionCondition.or) && op) {
                return true;
            }
        }
        return condition.equals(FunctionCondition.and);
    }

    private void invokeScript(AggregatedMetricEvent value, ExpressionFunction function, Map<String, Object> resultFields) {
        if (!StringUtil.isEmpty(function.getValueScript())) {
            try {
                ScriptInvoker scriptInvoker = ScriptInvokerManager.getValueInvoker(value.getMetadataId() + function.getField(), function.getValueScript());
                function.setValue(scriptInvoker.invoke(resultFields, value.getMetric().getTags()));
            } catch (Exception ex) {
                log.warn(String.format("Value script invoke fail. Script = [%s]", function.getValueScript()), ex);
            }
        }
    }
}
