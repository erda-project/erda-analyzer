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

import cloud.erda.analyzer.alert.models.NotifyHistory;
import cloud.erda.analyzer.alert.models.RenderedNotifyEvent;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;

public class NotifyHistoryMapFunction implements MapFunction<RenderedNotifyEvent, NotifyHistory> {

    @Override
    public NotifyHistory map(RenderedNotifyEvent renderedNotifyEvent) throws Exception {
        val metric = renderedNotifyEvent.getMetricEvent();
        val history = new NotifyHistory();
        history.setGroupId(renderedNotifyEvent.getNotifyTarget().getGroupId());
        history.setTimestamp(metric.getTimestamp() / (1000*1000));
        history.setTitle(renderedNotifyEvent.getTitle());
        history.setContent(renderedNotifyEvent.getContent());
        history.setContent(renderedNotifyEvent.getContent());
        return history;
    }
}
