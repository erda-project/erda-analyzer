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

package cloud.erda.analyzer.runtime.utils;

import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: liuhaoyang
 * @create: 2019-09-17 18:43
 **/
@Slf4j
public class AggregateUtils {

    public static String getAggregatedField(ExpressionFunction expressionFunction) {
        if (StringUtil.isNotEmpty(expressionFunction.getAlias())) {
            return expressionFunction.getAlias();
        }
        return expressionFunction.getField() + "_" + expressionFunction.getAggregator();
    }
}
