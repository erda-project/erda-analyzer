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

import cloud.erda.analyzer.runtime.models.ExpressionFunction;

import static cloud.erda.analyzer.runtime.expression.functions.operators.FunctionOperatorDefine.*;

/**
 * @author: liuhaoyang
 * @create: 2019-06-30 18:57
 **/
public class FunctionOperatorFactory {

    public static FunctionOperator create(ExpressionFunction function) {
        if (function.getOperator() == null) {
            return TrueFunctionOperator.getInstance();
        }
        switch (function.getOperator()) {
            case greaterThanOrEqual:
                return new GreaterThanOrEqualFunctionOperator();
            case greaterThan:
                return new GreaterThanFunctionOperator();
            case lessThanOrEqual:
                return new LessThanOrEqualFunctionOperator();
            case lessThan:
                return new LessThanFunctionOperator();
            case equal:
                return new EqualFunctionOperator();
            case notEqual:
                return new NotEqualFunctionOperator();
            case like:
                return new LikeFunctionOperator();
            case all:
                return new AllFunctionOperator();
            case any:
                return new AnyFunctionOperator();
            case contains:
                return new ContainsFunctionOperator();
            default:
                return new NoopFunctionOperator();
        }
    }
}
