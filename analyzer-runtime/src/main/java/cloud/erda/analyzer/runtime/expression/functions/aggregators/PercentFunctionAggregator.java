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

import cloud.erda.analyzer.runtime.utils.PercentUtils;
import cloud.erda.analyzer.common.utils.ConvertUtils;

import java.util.ArrayList;
import java.util.List;

public class PercentFunctionAggregator implements FunctionAggregator {

    /**
     * 表示百分位数，如果是 75 分位数则该值为 75
     */
    private double percent;

    private List<Double> data = new ArrayList<>();

    public PercentFunctionAggregator(String percent) {
        this.percent = Double.valueOf(percent.split("p")[1]);
    }

    @Override
    public String aggregator() {
        return "p" + percent;
    }

    @Override
    public Object value() {
        if (data.isEmpty()) {
            return null;
        }
        return PercentUtils.percent(data.stream().mapToDouble(Double::doubleValue).toArray(), percent);
    }

    @Override
    public void apply(Object value) {
        Double d = ConvertUtils.toDouble(value);
        if (d != null) {
            data.add(d);
        }
    }

    @Override
    public void merge(FunctionAggregator other) {
        if (other instanceof PercentFunctionAggregator) {
            this.data.addAll(((PercentFunctionAggregator) other).data);
        }
    }
}
