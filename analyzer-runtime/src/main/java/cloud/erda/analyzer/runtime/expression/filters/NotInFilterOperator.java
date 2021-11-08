/*
 * Copyright (c) 2021 Terminus, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.erda.analyzer.runtime.expression.filters;

import cloud.erda.analyzer.runtime.models.ExpressionFilter;

import java.util.List;

/**
 * @author liuhaoyang
 * @date 2021/11/8 16:10
 */
public class NotInFilterOperator implements FilterOperator{
    @Override
    public String operator() {
        return FilterOperatorDefine.NotIn;
    }

    @Override
    public boolean invoke(ExpressionFilter filter, Object value) {
        if (filter.getValue() == null) {
            return false;
        }
        if (!(filter.getValue() instanceof List)) {
            return false;
        }
        List<?> values = (List<?>) filter.getValue();
        return !values.contains(value);
    }

    public static final FilterOperator instance = new NotInFilterOperator();
}
