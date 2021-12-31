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

package cloud.erda.analyzer.runtime.sources;

import cloud.erda.analyzer.common.constant.ExpressionConstants;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.expression.filters.FilterOperatorDefine;
import cloud.erda.analyzer.runtime.models.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: liuhaoyang
 * @create: 2019-06-28 14:48
 **/
@Slf4j
public abstract class ExpressionMetadataReader implements DataRowReader<ExpressionMetadata> {

    @Override
    public ExpressionMetadata read(ResultSet resultSet) throws SQLException, IOException {
        try {

            ExpressionMetadata metadata = new ExpressionMetadata();
            metadata.setId(String.valueOf(resultSet.getInt("id")));
            metadata.setVersion(resultSet.getString("version"));
            metadata.setEnable(resultSet.getBoolean("enable"));
            /**
             * Expression 示例
             * {
             * 	"metric": "docker_container_cpu",
             * 	"window": 1,
             * 	"filter": {
             * 		"cluster_name": "terminus-y"
             *   },
             * 	"group": [
             * 		"host_ip",
             * 		"container_id"
             * 	],
             * 	"functions": [{
             * 		"aggregator": "sum",
             * 		"field": "usage_percent",
             * 		"operator": "gte",
             * 		"value": 10
             *    }],
             * 	"select": {
             * 		"cluster_name": "#cluster_name",
             * 		"custom_tag": "tagValue"
             *    }
             * }
             */

            Expression expression = parseJsonField(resultSet.getString("expression"), "expression", metadata.getId(), Expression.class);

            checkNotNull(expression, "Expression cannot be null");
            checkNotNull(expression.getWindow(), "Expression window cannot be null");

            metadata.setExpression(expression);

            /**
             * attributes 示例, alert_key alert_type notify_key 为必需项
             * {
             *  "alert_key":"terminus-",
             *  "alert_type":"xxx-xxx-xxx-xxx",
             *  "notify_key":"xxx-xxx-xxx-xxx"
             * }
             */

            String attributesValue = resultSet.getString("attributes");
            Map<String, Object> attributes = new HashMap<>();
            if (!StringUtils.isEmpty(attributesValue)) {
                Map<String, Object> local = parseJsonField(attributesValue, "attributes", metadata.getId(), String.class, Object.class);
                processAttributes(local, attributes);
            }
            attributes.put("window", String.valueOf(expression.getWindow()));

            metadata.setAttributes(attributes);

            checkExpression(metadata);

            metadata.setProcessingTime(System.currentTimeMillis());

            return metadata;
        } catch (Throwable throwable) {
            log.warn("Read or deserialize ExpressionMetadata error.", throwable);
            throw throwable;
        }
    }

    private void checkExpression(ExpressionMetadata metadata) {

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

    private <K, V> Map<K, V> parseJsonField(String value, String field, String matedataId, Class<K> keyClass, Class<V> valueClass) throws IOException {
        try {
            Map<K, V> map = JsonMapperUtils.toHashMap(value, keyClass, valueClass);
            return map;
        } catch (Throwable throwable) {
            log.warn("Parse json field fail. metadata {}, field {}\n {}", matedataId, field, value);
            throw throwable;
        }
    }

    private <V> V parseJsonField(String value, String field, String matedataId, Class<V> valueClass) throws IOException {
        try {
            V ins = JsonMapperUtils.toObject(value, valueClass);
            return ins;
        } catch (Throwable throwable) {
            log.warn("Parse json field fail. metadata {}, field {}\n {}", matedataId, field, value);
            throw throwable;
        }
    }

    private void processAttributes(Map<String, Object> src, Map<String, Object> des) {
        for (Map.Entry<String, Object> item : src.entrySet()) {
            if (item.getValue() instanceof String) {
                des.put(item.getKey(), (String) item.getValue());
            }
            if (item.getValue() instanceof List) {
                List<String> values = (List<String>) item.getValue();
                if (values.size() == 0) {
                    continue;
                }
                String value = values.stream().skip(1).reduce(values.get(0), (v1, v2) -> v1 + "," + v2);
                des.put(item.getKey(), value);
            }
        }
    }
}
