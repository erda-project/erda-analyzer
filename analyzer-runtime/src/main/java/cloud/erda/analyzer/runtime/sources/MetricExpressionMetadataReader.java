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

import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author: liuhaoyang
 * @create: 2019-11-26 00:25
 **/
@Slf4j
public class MetricExpressionMetadataReader extends ExpressionMetadataReader {

    @Override
    public ExpressionMetadata read(ResultSet resultSet) throws SQLException {
        try {
            ExpressionMetadata metadata = super.read(resultSet);
            if (metadata != null) {
                metadata.setId(String.format("metric_%s", metadata.getId()));
                log.info("Read metric metadata {}  expression: {}  attributes: {}", metadata.getId(), resultSet.getString("expression"), resultSet.getString("attributes"));
            }
            return metadata;
        } catch (Throwable ex) {
            log.warn("Read or deserialize ExpressionMetadata error.", ex);
            return null;
        }
    }
}
