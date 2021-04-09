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

package cloud.erda.analyzer.alert.models.eventbox;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

/**
 * @author: liuhaoyang
 * @create: 2020-01-19 14:49
 **/
@Data
public class EventBoxDingDingLabel {

    @SerializedName(value = "DINGDING")
    private String[] dingding;

    @SerializedName(value = "MARKDOWN")
    private EventBoxDingDingLabelMarkdown markdown;

    public static EventBoxDingDingLabel label(String title, String... dingding) {
        EventBoxDingDingLabel eventBoxDingDingLabel = new EventBoxDingDingLabel();
        eventBoxDingDingLabel.setDingding(dingding);
        EventBoxDingDingLabelMarkdown eventBoxDingDingLabelMarkdown = new EventBoxDingDingLabelMarkdown();
        eventBoxDingDingLabelMarkdown.setTitle(title);
        eventBoxDingDingLabel.setMarkdown(eventBoxDingDingLabelMarkdown);
        return eventBoxDingDingLabel;
    }

    @Data
    public static class EventBoxDingDingLabelMarkdown {
        private String title;
    }
}
