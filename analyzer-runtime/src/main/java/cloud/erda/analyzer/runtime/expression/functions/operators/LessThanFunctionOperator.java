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
import cloud.erda.analyzer.common.utils.ConvertUtils;

import static cloud.erda.analyzer.runtime.expression.functions.operators.FunctionOperatorDefine.lessThan;

public class LessThanFunctionOperator implements FunctionOperator {

    @Override
    public String operator() {
        return lessThan;
    }

    @Override
    public boolean invoke(AggregateResult aggregateResult) throws Exception {
        Double d = ConvertUtils.toDouble(aggregateResult.getValue());
        Double value = ConvertUtils.toDouble(aggregateResult.getFunction().getValue());
        if (d == null || value == null) {
            return false;
        }
        return d < value;
    }
}
