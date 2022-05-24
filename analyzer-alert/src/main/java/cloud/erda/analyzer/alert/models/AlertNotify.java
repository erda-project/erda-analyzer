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

package cloud.erda.analyzer.alert.models;

import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.Data;

/**
 * @author: liuhaoyang
 * @create: 2020-01-01 23:50
 **/
@Data
public class AlertNotify {

    private long id;
    @JsonSetter("alert_id")
    private String alertId;
    @JsonSetter("notify_target")
    private AlertNotifyTarget notifyTarget;

    private long silence;
    @JsonSetter("silence_policy")
    private String silencePolicy;

    private boolean enable;

    private long processingTime;

    private String updated;
}
