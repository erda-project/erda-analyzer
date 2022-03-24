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

package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.AlertLevel;
import cloud.erda.analyzer.alert.models.RenderedAlertEvent;
import cloud.erda.analyzer.alert.models.AlertRecord;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author randomnil
 */
public class AlertRecordMapFunction implements MapFunction<RenderedAlertEvent, AlertRecord> {

    @Override
    public AlertRecord map(RenderedAlertEvent value) throws Exception {
        val metric = value.getMetricEvent();
        val record = new AlertRecord();
        record.setGroupId(this.getTag(metric, AlertConstants.ALERT_GROUP_ID));
        record.setScope(this.getTag(metric, AlertConstants.ALERT_SCOPE));
        record.setScopeKey(this.getTag(metric, AlertConstants.ALERT_SCOPE_ID));
        record.setAlertGroup(this.getTag(metric, AlertConstants.ALERT_GROUP));
        record.setTitle(value.getTitle());
        record.setAlertState(this.getTag(metric, AlertConstants.TRIGGER));
        record.setAlertType(this.getTag(metric, AlertConstants.ALERT_TYPE));
        record.setAlertIndex(this.getTag(metric, AlertConstants.ALERT_INDEX));
        record.setExpressionKey(this.getTag(metric, AlertConstants.ALERT_EXPRESSION_ID));
        record.setAlertId(Long.valueOf(this.getTag(metric, AlertConstants.ALERT_ID)));
        record.setAlertName(this.getTag(metric, AlertConstants.ALERT_TITLE));
        record.setRuleId(Long.valueOf(this.getTag(metric, AlertConstants.ALERT_RULE_ID)));
        record.setRuleName(this.getTag(metric, AlertConstants.ALERT_RULE_NAME));
        record.setAlertSource(this.getTag(metric, AlertConstants.ALERT_SOURCE));
        record.setAlertSubject(this.getTag(metric, AlertConstants.ALERT_SUBJECT));
        record.setAlertEventFamilyId(this.getTag(metric, AlertConstants.ALERT_EVENT_FAMILY_ID));
        record.setOrgId(Long.valueOf(this.getTag(metric, AlertConstants.DICE_ORG_ID)));
        record.setAlertLevel(AlertLevel.of(this.getTag(metric, AlertConstants.ALERT_LEVEL)).toString());
        record.setAlertTime(metric.getTimestamp() / (1000 * 1000));
        return record;
    }

    private String getTag(MetricEvent metric, String key) {
        val value = metric.getTags().get(key);
        if (value == null) {
            return StringUtils.EMPTY;
        }
        return value;
    }
}