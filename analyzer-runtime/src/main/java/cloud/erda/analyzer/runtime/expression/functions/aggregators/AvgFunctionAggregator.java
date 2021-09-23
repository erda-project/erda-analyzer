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

public class AvgFunctionAggregator implements FunctionAggregator {

    private double sum = 0d;
    private long count = 0l;

    @Override
    public String aggregator() {
        return FunctionAggregatorDefine.AVG;
    }

    @Override
    public Object value() {
        if (count == 0) {
            return null;
        }
        return sum / count;
    }

    @Override
    public void apply(Object value) {
        Double d = ConvertUtils.toDouble(value);
        if (d != null) {
            sum += d;
            count++;
        }
    }

    @Override
    public void merge(FunctionAggregator other) {
        if (other instanceof AvgFunctionAggregator) {
            this.count += ((AvgFunctionAggregator) other).count;
            this.sum += ((AvgFunctionAggregator) other).sum;
        }
    }
}
