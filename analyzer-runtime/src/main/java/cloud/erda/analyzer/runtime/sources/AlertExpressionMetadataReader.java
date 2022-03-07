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

import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: liuhaoyang
 * @create: 2019-11-25 17:18
 **/
@Slf4j
public class AlertExpressionMetadataReader extends ExpressionMetadataReader {

    @Override
    public ExpressionMetadata read(ResultSet resultSet) throws SQLException {
        try {
            ExpressionMetadata expressionMetadata = super.read(resultSet);
            expressionMetadata.setId(String.format("alert_%s", expressionMetadata.getId()));
            Map<String, String> attributes = expressionMetadata.getAttributes();
            checkNotNull(attributes.get(AlertConstants.ALERT_INDEX), "Attribute alert_index cannot be null");
            checkNotNull(attributes.get(AlertConstants.ALERT_TYPE), "Attribute alert_type cannot be null");
//            checkNotNull(attributes.get(DICE_ORG_ID), "Attribute dice_org_id cannot be null");
//            checkNotNull(attributes.get(ALERT_GROUP), "Attribute alert_group cannot be null");
            attributes.put(AlertConstants.ALERT_ID, String.valueOf(resultSet.getInt("alert_id")));
            log.info("Read alert metadata {}  expression: {}  attributes: {}", expressionMetadata.getId(), resultSet.getString("expression"), resultSet.getString("attributes"));
            return expressionMetadata;
        } catch (Exception ex) {
            log.warn("Read or deserialize ExpressionMetadata error.", ex);
            return null;
        }
    }
}
