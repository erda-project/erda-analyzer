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
import cloud.erda.analyzer.runtime.scripts.ScriptInvoker;
import cloud.erda.analyzer.runtime.scripts.ScriptInvokerManager;

/**
 * @author: liuhaoyang
 * @create: 2019-12-18 11:47
 **/
public class ScriptFilterOperator implements FilterOperator {

    private String metadataId;

    public ScriptFilterOperator(String metadataId) {
        this.metadataId = metadataId;
    }

    @Override
    public String operator() {
        return FilterOperatorDefine.Script;
    }

    @Override
    public boolean invoke(ExpressionFilter filter, Object value) {
        if (filter.getValue() == null || !(filter.getValue() instanceof String)) {
            return false;
        }
        try {
            ScriptInvoker invoker = ScriptInvokerManager.getFieldInvoker(metadataId + filter.getTag(), String.valueOf(filter.getValue()));
            Object result = invoker.invoke(value);
            if (result == null) {
                return false;
            }
            return Boolean.valueOf(String.valueOf(result));
        } catch (Exception ex) {
            return false;
        }
    }
}
