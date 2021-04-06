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
import java.util.HashSet;

/**
 * @author: recallsong
 * @create: 2020-05-18 11:00
 **/
public class DistinctFunctionAggregator implements FunctionAggregator {

    private final HashSet<Object> set = new HashSet<>();

    @Override
    public String aggregator() {
        return FunctionAggregatorDefine.DISTINCT;
    }

    @Override
    public Object value() {
        return new ArrayList(this.set);
    }

    @Override
    public void apply(Object value) {
        if (value != null) {
            set.add(value);
        }
    }

    @Override
    public void merge(FunctionAggregator other) {
        if (other instanceof DistinctFunctionAggregator) {
            HashSet<Object> set = ((DistinctFunctionAggregator) other).set;
            this.set.addAll(set);
        }
    }
}
