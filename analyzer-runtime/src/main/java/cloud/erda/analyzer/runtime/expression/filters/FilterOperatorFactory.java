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

package cloud.erda.analyzer.runtime.expression.filters;

import cloud.erda.analyzer.runtime.models.ExpressionFilter;
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;

/**
 * @author: liuhaoyang
 * @create: 2019-07-12 10:46
 **/
public class FilterOperatorFactory {

    public static FilterOperator create(ExpressionFilter filter, ExpressionMetadata metadata) {
        switch (filter.getOperator()) {
            case FilterOperatorDefine.Equal:
                return EqualFilterOperator.instance;
            case FilterOperatorDefine.NotEqual:
                return NotEqualFilterOperator.instance;
            case FilterOperatorDefine.Like:
                return LikeFilterOperator.instance;
            case FilterOperatorDefine.Any:
                return AnyFilterOperator.instance;
            case FilterOperatorDefine.Null:
                return NullFilterOperator.instance;
            case FilterOperatorDefine.In:
                return InFilterOperator.instance;
            case FilterOperatorDefine.NotIn:
                return NotInFilterOperator.instance;
            case FilterOperatorDefine.Match:
                return MatchFilterOperator.INSTANCE;
            case FilterOperatorDefine.NotMatch:
                return NotMatchFilterOperator.INSTANCE;
            case FilterOperatorDefine.Script:
                return new ScriptFilterOperator(metadata.getId());
            case FilterOperatorDefine.False:
            default:
                return FalseFilterOperator.instance;
        }
    }
}
