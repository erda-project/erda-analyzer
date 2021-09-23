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
import lombok.extern.slf4j.Slf4j;

/**
 * @author: liuhaoyang
 * @create: 2019-06-30 00:23
 **/
@Slf4j
public class SumFunctionAggregator implements FunctionAggregator {

    private double sum = 0d;

    @Override
    public String aggregator() {
        return FunctionAggregatorDefine.SUM;
    }

    @Override
    public Object value() {
        return sum;
    }

    @Override
    public void apply(Object value) {
        Double d = ConvertUtils.toDouble(value);
        if (d != null) {
            sum += d;
        }
    }

    @Override
    public void merge(FunctionAggregator other) {
        if (other instanceof SumFunctionAggregator) {
            this.sum += ((SumFunctionAggregator) other).sum;
        }
    }
}
