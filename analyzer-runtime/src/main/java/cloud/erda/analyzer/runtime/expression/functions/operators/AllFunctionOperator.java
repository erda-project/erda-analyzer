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

package cloud.erda.analyzer.runtime.expression.functions.operators;

import cloud.erda.analyzer.runtime.models.AggregateResult;

import java.util.Collection;

/**
 * @author: liuhaoyang
 * @create: 2019-12-27 10:54
 **/
public class AllFunctionOperator implements FunctionOperator {
    @Override
    public String operator() {
        return FunctionOperatorDefine.all;
    }

    @Override
    public boolean invoke(AggregateResult aggregateResult) throws Exception {
        Object value = aggregateResult.getFunction().getValue();
        if (value == null) {
            return false;
        }
        Object result = aggregateResult.getValue();
        if (!(result instanceof Collection)) {
            return false;
        }
        Collection collection = (Collection) result;
        if (collection.isEmpty()) {
            return false;
        }
        for (Object item : collection) {
            if (!value.equals(item)) {
                return false;
            }
        }
        return true;
    }
}
