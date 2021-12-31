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

package cloud.erda.analyzer.runtime.models;

import cloud.erda.analyzer.common.constant.ExpressionConstants;
import cloud.erda.analyzer.runtime.expression.filters.FilterOperatorDefine;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: liuhaoyang
 * @create: 2019-06-28 14:34
 **/
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExpressionMetadata {

    private String id;

    private String version;

    private Map<String, Object> attributes;

    private Expression expression;

    private boolean enable;

    private long processingTime;

    public void checkExpression(ExpressionMetadata metadata) {

        Expression expression = metadata.getExpression();

        if (expression.getMetrics() == null) {
            expression.setMetrics(new ArrayList<>());
        }

        if (expression.getFilters() == null) {
            expression.setFilters(new ArrayList<>());
        }

        if (expression.getGroup() == null) {
            expression.setGroup(new ArrayList<>());
        }

        if (expression.getFunctions() == null) {
            expression.setFunctions(new ArrayList<>());
        }

        if (expression.getSelect() == null) {
            expression.setSelect(new HashMap<>());
        }

        if (expression.getCondition() == null) {
            expression.setCondition(FunctionCondition.and);
        }

        if (expression.getWindowBehavior() == null) {
            expression.setWindowBehavior(WindowBehavior.none);
        }

        for (ExpressionFunction function : expression.getFunctions()) {
            function.setCondition(expression.getCondition());
            if (function.getTrigger() == null) {
                function.setTrigger(ExpressionFunctionTrigger.applied);
            }
        }

        if (ExpressionConstants.EXPRESSION_VERSION_1_0.equals(metadata.getVersion())) {
            List<String> outputs = expression.getOutputs();
            if (outputs == null) {
                outputs = new ArrayList<>();
            }
            if (!outputs.contains(ExpressionConstants.OUTPUT_ALERT)) {
                outputs.add(ExpressionConstants.OUTPUT_ALERT);
            }
            expression.setOutputs(outputs);
        }

        if (ExpressionConstants.EXPRESSION_VERSION_1_0.equals(metadata.getVersion()) || ExpressionConstants.EXPRESSION_VERSION_2_0.equals(metadata.getVersion())) {
            Map<String, Object> filter = expression.getFilter();
            if (filter != null) {
                for (Map.Entry<String, Object> entry : filter.entrySet()) {
                    Object value = entry.getValue();
                    if (value instanceof String) {
                        ExpressionFilter expressionFilter = new ExpressionFilter();
                        expressionFilter.setTag(entry.getKey());
                        expressionFilter.setValue(value);
                        expressionFilter.setOperator(FilterOperatorDefine.Equal);
                        expression.getFilters().add(expressionFilter);
                    }
                }
            }
        }

        if (expression.getMetric() != null) {
            if (!expression.getMetrics().contains(expression.getMetric())) {
                expression.getMetrics().add(expression.getMetric());
            }
        }

        if (expression.getAlias() == null) {
            if (expression.getMetrics().size() == 1) {
                expression.setAlias(expression.getMetrics().get(0));
            } else {
                expression.setAlias(String.join("_", expression.getMetrics()));
            }
        }
        checkNotNull(expression.getOutputs(), "Expression outputs cannot be null");
        checkNotNull(expression.getAlias(), "Expression alias cannot be null");
    }

}


