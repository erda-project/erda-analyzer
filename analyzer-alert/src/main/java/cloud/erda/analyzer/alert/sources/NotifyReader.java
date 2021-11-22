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

import cloud.erda.analyzer.alert.models.AlertLevel;
import cloud.erda.analyzer.alert.models.AlertNotify;
import cloud.erda.analyzer.alert.models.AlertNotifyTarget;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.utils.GsonUtil;
import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.runtime.sources.DataRowReader;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;

/**
 * @author: liuhaoyang
 * @create: 2020-01-03 14:25
 **/
@Slf4j
public class NotifyReader implements DataRowReader<AlertNotify> {

    @Override
    public AlertNotify read(ResultSet resultSet) throws Exception {
        try {
            AlertNotify notify = new AlertNotify();
            notify.setId(resultSet.getLong("id"));
            notify.setAlertId(resultSet.getString("alert_id"));
            notify.setEnable(resultSet.getBoolean("enable"));
            String notifyTargetData = resultSet.getString("notify_target");
            AlertNotifyTarget notifyTarget = GsonUtil.toObject(notifyTargetData, AlertNotifyTarget.class);
            if (AlertConstants.ALERT_NOTIFY_TYPE_NOTIFY_GROUP.equals(notifyTarget.getType())) {
                notifyTarget.setGroupTypes(notifyTarget.getGroupType().split(","));
            }
            if (StringUtil.isNotEmpty(notifyTarget.getLevel())) {
                String[] levelStrs = notifyTarget.getLevel().split(",");
                AlertLevel[] levels = new AlertLevel[levelStrs.length];
                for (int i = 0; i < levelStrs.length; i++) {
                    levels[i] = AlertLevel.of(levelStrs[i]);
                }
                notifyTarget.setLevels(levels);
            } else {
                notifyTarget.setLevels(new AlertLevel[0]);
            }
            notify.setNotifyTarget(notifyTarget);
            notify.setSilence(resultSet.getLong("silence"));
            notify.setSilencePolicy(resultSet.getString("silence_policy"));
            notify.setProcessingTime(System.currentTimeMillis());
            log.info("Read alert notify {}  data: {}", notify.getId(), GsonUtil.toJson(notify));
            return notify;
        } catch (Exception ex) {
            log.warn("Read or deserialize Notify Metadata error.", ex);
            return null;
        }
    }
}
