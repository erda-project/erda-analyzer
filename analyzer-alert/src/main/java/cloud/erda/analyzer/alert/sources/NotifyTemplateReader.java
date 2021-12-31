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

package cloud.erda.analyzer.alert.sources;

import cloud.erda.analyzer.alert.models.AlertNotifyTemplate;
import cloud.erda.analyzer.alert.models.AlertTrigger;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.sources.DataRowReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: liuhaoyang
 * @create: 2020-01-03 14:29
 **/
@Slf4j
public class NotifyTemplateReader implements DataRowReader<AlertNotifyTemplate> {

    private boolean templateVariable;

    public NotifyTemplateReader(boolean templateVariable) {
        this.templateVariable = templateVariable;
    }

    @Override
    public AlertNotifyTemplate read(ResultSet resultSet) throws Exception {
        try {
            AlertNotifyTemplate notifyTemplate = new AlertNotifyTemplate();
//            notifyTemplate.setId(resultSet.getLong("id"));
            notifyTemplate.setName(resultSet.getString("name"));
            notifyTemplate.setAlertType(resultSet.getString("alert_type"));
            notifyTemplate.setAlertIndex(resultSet.getString("alert_index"));
            notifyTemplate.setTarget(resultSet.getString("target"));
            notifyTemplate.setTrigger(AlertTrigger.valueOf(resultSet.getString("trigger")));
            notifyTemplate.setTitle(resultSet.getString("title"));
            notifyTemplate.setTemplate(resultSet.getString("template"));
            notifyTemplate.setEnable(resultSet.getBoolean("enable"));
            String formatString = resultSet.getString("formats");
            Map<String, String> formats = null;
            if (StringUtils.isEmpty(formatString)) {
                formats = new HashMap<>();
            } else {
                formats = JsonMapperUtils.toStringValueMap(formatString);
            }
            notifyTemplate.setFormats(formats);
            checkNotNull(notifyTemplate.getTitle(), "Title cannot be null");
            checkNotNull(notifyTemplate.getTemplate(), "Template cannot be null");
            notifyTemplate.setProcessingTime(System.currentTimeMillis());
            notifyTemplate.setVariable(templateVariable);
            log.info("Read alert notify template {} data: {}",notifyTemplate.getAlertIndex(), JsonMapperUtils.toStrings(notifyTemplate));
            return notifyTemplate;
        } catch (Exception ex) {
            log.warn("Read or deserialize id {} Custom AlertNotifyTemplate error.", resultSet.getLong("id"), ex);
            return null;
        }
    }
}
