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

import cloud.erda.analyzer.common.utils.ConvertUtils;

public class MinFunctionAggregator implements FunctionAggregator {

    private Double min = null;

    @Override
    public String aggregator() {
        return FunctionAggregatorDefine.MIN;
    }

    @Override
    public Object value() {
        return min;
    }

    @Override
    public void apply(Object value) {
        Double d = ConvertUtils.toDouble(value);
        if (d != null && (min == null || d < min)) {
            min = d;
        }
    }

    @Override
    public void merge(FunctionAggregator other) {
        if (other instanceof MinFunctionAggregator) {
            if (((MinFunctionAggregator) other).min < min) {
                this.min = ((MinFunctionAggregator) other).min;
            }
        }
    }
}
