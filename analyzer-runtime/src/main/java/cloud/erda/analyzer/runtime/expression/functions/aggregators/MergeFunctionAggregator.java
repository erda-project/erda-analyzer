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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author liuhaoyang
 * @date 2020/3/26 17:25
 */
public class MergeFunctionAggregator implements FunctionAggregator {

    private final Set<Object> values = new HashSet<>();

    @Override
    public String aggregator() {
        return FunctionAggregatorDefine.MERGE;
    }

    @Override
    public Object value() {
        return new ArrayList<>(values);
    }

    @Override
    public void apply(Object value) {
        if (value != null) {
            if (value instanceof Collection) {
                this.values.addAll((Collection) value);
            } else {
                this.values.add(value);
            }
        }
    }

    @Override
    public void merge(FunctionAggregator other) {
        this.apply(other.value());
    }
}
