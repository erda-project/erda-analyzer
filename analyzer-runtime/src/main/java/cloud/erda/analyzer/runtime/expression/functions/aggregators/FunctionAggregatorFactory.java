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

package cloud.erda.analyzer.runtime.expression.functions.aggregators;

import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import cloud.erda.analyzer.runtime.models.ExpressionFunctionTrigger;
import lombok.extern.slf4j.Slf4j;

import static cloud.erda.analyzer.runtime.expression.functions.aggregators.FunctionAggregatorDefine.*;

/**
 * @author: liuhaoyang
 * @create: 2019-06-30 23:12
 **/
@Slf4j
public class FunctionAggregatorFactory {

    public static FunctionAggregator create(ExpressionFunction function) {

        FunctionAggregator aggregator = null;

        switch (function.getAggregator()) {
            case SUM:
                aggregator = new SumFunctionAggregator();
                break;
            case COUNT:
                aggregator = new CountFunctionAggregator();
                break;
            case AVG:
                aggregator = new AvgFunctionAggregator();
                break;
            case MAX:
                aggregator = new MaxFunctionAggregator();
                break;
            case MIN:
                aggregator = new MinFunctionAggregator();
                break;
            case VALUE:
                aggregator = new ValueFunctionAggregator();
                break;
            case VALUES:
                aggregator = new ValuesFunctionAggregator();
                break;
            case DISTINCT:
                aggregator = new DistinctFunctionAggregator();
                break;
            case MERGE:
                aggregator = new MergeFunctionAggregator();
                break;
            case P50:
            case P75:
            case P90:
            case P95:
            case P99:
                //百分位计算的按 p99 配置规则
                aggregator = new PercentFunctionAggregator(function.getAggregator());
                break;
            default:
                /**
                 * 如果用户定义了未实现的聚合器，默认使用空聚合器，业务容错避免抛出异常影响其他的表达式执行
                 */
                log.warn("The aggregator {} is undefined, will use {}", function.getAggregator(), NoopFunctionAggregator.class);
                aggregator = new NoopFunctionAggregator();
                break;
        }

        if (ExpressionFunctionTrigger.applied.equals(function.getTrigger())) {
            aggregator = new AppliedFunctionAggregator(aggregator);
        }

        return aggregator;
    }
}
