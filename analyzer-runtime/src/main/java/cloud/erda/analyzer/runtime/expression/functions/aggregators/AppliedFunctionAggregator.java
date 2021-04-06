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

/**
 * @author: liuhaoyang
 * @create: 2019-12-19 23:01
 **/
public class AppliedFunctionAggregator implements FunctionAggregator {

    private final FunctionAggregator internalAggregator;
    private Object value;
    private boolean hasValue = false;

    public AppliedFunctionAggregator(FunctionAggregator internalAggregator) {
        this.internalAggregator = internalAggregator;
    }

    @Override
    public String aggregator() {
        return internalAggregator.aggregator();
    }

    @Override
    public Object value() {
        if (!hasValue) {
            value = internalAggregator.value();
            hasValue = true;
        }
        return value;
    }

    @Override
    public void apply(Object value) {
        internalAggregator.apply(value);
    }

    @Override
    public void merge(FunctionAggregator other) {
        internalAggregator.merge(other);
    }

    @Override
    public int hashCode() {
        return internalAggregator.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AppliedFunctionAggregator) {
            return internalAggregator.equals(((AppliedFunctionAggregator) obj).internalAggregator);
        }
        return internalAggregator.equals(obj);
    }
}
