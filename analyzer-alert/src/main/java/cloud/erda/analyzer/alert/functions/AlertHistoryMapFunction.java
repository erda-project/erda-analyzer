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
import cloud.erda.analyzer.common.models.EventKind;
import cloud.erda.analyzer.common.models.EventNameConstants;
import cloud.erda.analyzer.common.models.Relation;
import cloud.erda.analyzer.common.models.RelationTypeConstants;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * @author randomnil
 */
public class AlertHistoryMapFunction implements MapFunction<RenderedAlertEvent, AlertHistory> {

    @Override
    public AlertHistory map(RenderedAlertEvent value) throws Exception {
        val metric = value.getMetricEvent();
        val history = new AlertHistory();

        history.setTimeUnixNano(metric.getTimestamp());
        history.setKind(EventKind.EVENT_KIND_ALERT);
        history.setName(EventNameConstants.ALERT);
        history.setMessage(value.getContent());

        Relation relation = new Relation();
        relation.setResID(metric.getTags().get(AlertConstants.ALERT_GROUP_ID));
        relation.setResType(RelationTypeConstants.ALERT);
        history.setRelations(relation);

        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(AlertConstants.ALERT_TITLE, value.getTitle());
        attributes.put(AlertConstants.TRIGGER, metric.getTags().get(AlertConstants.TRIGGER));
        attributes.put(AlertConstants.DISPLAY_URL, metric.getTags().get(AlertConstants.DISPLAY_URL));
        attributes.put(AlertConstants.ORG_NAME, metric.getTags().get(AlertConstants.ORG_NAME));
        attributes.put(AlertConstants.DICE_ORG_ID, metric.getTags().getOrDefault(AlertConstants.DICE_ORG_ID, AlertConstants.INVALID_ORG_ID));
        history.setAttributes(attributes);

        return history;
    }
}