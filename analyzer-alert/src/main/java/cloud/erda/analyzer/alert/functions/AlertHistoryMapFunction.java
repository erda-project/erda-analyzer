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

import cloud.erda.analyzer.alert.models.AlertHistory;
import cloud.erda.analyzer.alert.models.RenderedAlertEvent;
import cloud.erda.analyzer.common.constant.AlertConstants;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author randomnil
 */
public class AlertHistoryMapFunction implements MapFunction<RenderedAlertEvent, AlertHistory> {

    @Override
    public AlertHistory map(RenderedAlertEvent value) throws Exception {
        val metric = value.getMetricEvent();
        val history = new AlertHistory();
        history.setGroupId(metric.getTags().get(AlertConstants.ALERT_GROUP_ID));
        history.setTimestamp(metric.getTimestamp() / (1000 * 1000));
        history.setAlertState(metric.getTags().get(AlertConstants.TRIGGER));
        history.setTitle(value.getTitle());
        history.setContent(value.getContent());
        history.setDisplayUrl(metric.getTags().get(AlertConstants.DISPLAY_URL));

        return history;
    }
}