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

import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.Expression;
import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import cloud.erda.analyzer.common.models.MetricEvent;

import java.io.Serializable;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 20:58
 **/
public interface Accumulator extends Serializable {

    FunctionAggregator getMetricAggregator(ExpressionFunction function);

    boolean initialized();

    void setMetadataId(String id);

    void setFunction(FunctionAggregator aggregator, ExpressionFunction expression);

    void setMetric(MetricEvent metricEvent);

    void setKey(String key);

    void setAttribute(String key, Object value);

    void setExpression(Expression expression);

    String getMetadataId();

    Expression getExpression();

    MetricEvent getMetric();

    AggregatedMetricEvent aggregate();

    Accumulator merge(Accumulator other);
}
