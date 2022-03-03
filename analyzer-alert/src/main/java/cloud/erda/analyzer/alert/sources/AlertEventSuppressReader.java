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

import cloud.erda.analyzer.alert.models.AlertEventSuppress;
import cloud.erda.analyzer.alert.models.AlertSuppressType;
import cloud.erda.analyzer.alert.models.Notify;
import cloud.erda.analyzer.alert.models.NotifyTarget;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.functions.MetricAlertSelectFunction;
import cloud.erda.analyzer.runtime.sources.DataRowReader;
import org.apache.kafka.common.protocol.types.Field;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;

public class AlertEventSuppressReader implements DataRowReader<AlertEventSuppress> {

    @Override
    public AlertEventSuppress read(ResultSet resultSet) throws Exception {
        String id = resultSet.getString("id");
        String alertEventId = resultSet.getString("alert_event_id");
        AlertSuppressType suppressType = AlertSuppressType.valueOf(resultSet.getString("suppress_type"));
        Date expireTime = resultSet.getTimestamp("expire_time");
        boolean enabled = resultSet.getBoolean("enabled");

        AlertEventSuppress suppress = new AlertEventSuppress();
        suppress.setId(id);
        suppress.setAlertEventId(alertEventId);
        suppress.setSuppressType(suppressType);
        suppress.setExpireTime(expireTime);
        suppress.setEnabled(enabled);
        return suppress;
    }
}
