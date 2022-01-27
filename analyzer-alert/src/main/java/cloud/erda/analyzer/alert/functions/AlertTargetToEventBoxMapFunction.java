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

import cloud.erda.analyzer.alert.models.RenderedAlertEvent;
import cloud.erda.analyzer.alert.models.eventbox.*;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-01-07 13:48
 **/
public class AlertTargetToEventBoxMapFunction implements MapFunction<RenderedAlertEvent, EventBoxRequest> {

    @Override
    public EventBoxRequest map(RenderedAlertEvent value) throws Exception {
        EventBoxRequest request = new EventBoxRequest();
        request.setSender(Constants.EVENTBOX_SENDER);

        if (value.getNotifyTarget().getType().equals(AlertConstants.ALERT_NOTIFY_TYPE_DINGDING)) {
            request.setContent(value.getContent());
            EventBoxDingDingLabel eventBoxDingDingLabel = EventBoxDingDingLabel.label(value.getTitle(), value.getNotifyTarget().getDingdingUrl());
            request.setLabels(eventBoxDingDingLabel);
        } else {
            Map<String, String> tags = value.getMetricEvent().getTags();
            EventBoxContent content = new EventBoxContent();
            content.setSourceType(tags.get(AlertConstants.ALERT_SCOPE));
            content.setSourceId(tags.get(AlertConstants.ALERT_SCOPE_ID));
            content.setNotifyItemDisplayName(value.getTitle());
            content.setOrgId(Integer.parseInt(tags.getOrDefault(AlertConstants.DICE_ORG_ID, AlertConstants.INVALID_ORG_ID)));
            content.setAlertId(Integer.parseInt(tags.getOrDefault(AlertConstants.ALERT_ID, AlertConstants.INVALID_ORG_ID)));
//            String[] groupTypes = value.getNotifyTarget().getGroupTypes();
//            for (String groupType : groupTypes) {
//                EventBoxChannel eventBoxChannel = new EventBoxChannel();
//                eventBoxChannel.setName(groupType);
//                eventBoxChannel.setTemplate(value.getContent());
//                // todo 只在3.11兼容邮件模板用
//                if (groupType.equals("email")) {
//                    eventBoxChannel.setType("markdown");
//                }
//                content.getChannels().add(eventBoxChannel);
//            }
            String groupType = value.getTemplateTarget();
            EventBoxChannel eventBoxChannel = new EventBoxChannel();
            eventBoxChannel.setName(groupType);
            eventBoxChannel.setTemplate(value.getContent());
            // todo 在 3.19 兼容tag使用，eventbox Channel 需要重构，tag 改为map比较好
            eventBoxChannel.setTag(JsonMapperUtils.toStrings(value.getMetricEvent().getTags()));
            // todo 只在3.11兼容邮件模板用
            if (groupType.equals("email")) {
                eventBoxChannel.setType("markdown");
            }
            content.getChannels().add(eventBoxChannel);
            request.setContent(content);
            EventBoxNotifyGroupLabel eventBoxNotifyGroupLabel = new EventBoxNotifyGroupLabel();
            eventBoxNotifyGroupLabel.setGroup(Long.parseLong(value.getNotifyTarget().getGroupId()));
            request.setLabels(eventBoxNotifyGroupLabel);
        }
        return request;
    }
}

