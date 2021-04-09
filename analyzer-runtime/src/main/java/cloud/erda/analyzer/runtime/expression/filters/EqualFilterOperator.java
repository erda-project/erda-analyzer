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

/**
 * @author: liuhaoyang
 * @create: 2019-07-12 10:40
 **/
public class EqualFilterOperator implements FilterOperator {

    @Override
    public String operator() {
        return FilterOperatorDefine.Equal;
    }

    @Override
    public boolean invoke(ExpressionFilter filter, Object value) {
        if (filter.getValue() == null) {
            return value == null;
        } else {
            return filter.getValue().equals(value);
        }
    }

    public static final FilterOperator instance = new EqualFilterOperator();
}
