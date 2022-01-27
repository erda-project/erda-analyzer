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

import cloud.erda.analyzer.alert.models.RenderedNotifyEvent;
import cloud.erda.analyzer.alert.models.eventbox.*;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

public class NotifyTargetToEventBoxMapFunction implements MapFunction<RenderedNotifyEvent, EventBoxRequest> {
    @Override
    public EventBoxRequest map(RenderedNotifyEvent renderedNotifyEvent) throws Exception {
        EventBoxRequest request = new EventBoxRequest();
        request.setSender(Constants.EVENTBOX_SENDER);
        if (renderedNotifyEvent.getNotifyTarget().equals(AlertConstants.ALERT_NOTIFY_TYPE_DINGDING)) {
            request.setContent(renderedNotifyEvent.getContent());
            EventBoxDingDingLabel eventBoxDingDingLabel = EventBoxDingDingLabel.label(renderedNotifyEvent.getTitle(), renderedNotifyEvent.getNotifyTarget().getDingdingUrl());
            request.setLabels(eventBoxDingDingLabel);
        } else {
            Map<String, String> tags = renderedNotifyEvent.getMetricEvent().getTags();
            EventBoxContent content = new EventBoxContent();
//            content.setSourceType(renderedNotifyEvent.getScopeType());
//            content.setSourceId(renderedNotifyEvent.getScopeId());
            content.setNotifyItemDisplayName(renderedNotifyEvent.getTitle());
            content.setOrgId(Integer.parseInt(tags.getOrDefault(AlertConstants.DICE_ORG_ID, AlertConstants.INVALID_ORG_ID)));
            content.setAlertId(Integer.parseInt(tags.getOrDefault(AlertConstants.ALERT_ID, AlertConstants.INVALID_ORG_ID)));
            String groupType = renderedNotifyEvent.getTemplateTarget();
            EventBoxChannel eventBoxChannel = new EventBoxChannel();
            eventBoxChannel.setName(groupType);
            eventBoxChannel.setTemplate(renderedNotifyEvent.getContent());
            eventBoxChannel.setTag(JsonMapperUtils.toStrings(renderedNotifyEvent.getMetricEvent().getTags()));
            if (groupType.equals("email")) {
                eventBoxChannel.setType("markdown");
            }
            content.getChannels().add(eventBoxChannel);
            request.setContent(content);
            EventBoxNotifyGroupLabel eventBoxNotifyGroupLabel = new EventBoxNotifyGroupLabel();
            eventBoxNotifyGroupLabel.setGroup(Long.valueOf(renderedNotifyEvent.getNotifyTarget().getGroupId()));
            request.setLabels(eventBoxNotifyGroupLabel);
        }
        return request;
    }
}
