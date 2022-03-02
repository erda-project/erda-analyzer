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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.Data;

import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-01-02 16:44
 **/
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertNotifyTemplate {

    private String id;

    private String name;
    @JsonSetter("alert_type")
    private String alertType;
    @JsonSetter("alert_index")
    private String alertIndex;

    private String target;

    private AlertTrigger trigger;

    private String title;

    private String template;

    private Map<String, String> formats;

    private String language;

    /**
     * 内置的告警模板是不可变的，可以缓存解析器
     * 自定义告警模板是可变的。
     */
    private boolean variable;

    private boolean enable;

    private long processingTime;

    public AlertNotifyTemplate copy() {
        AlertNotifyTemplate alertNotifyTemplate = new AlertNotifyTemplate();
        alertNotifyTemplate.setId(id);
        alertNotifyTemplate.setName(name);
        alertNotifyTemplate.setAlertType(alertType);
        alertNotifyTemplate.setAlertIndex(alertIndex);
        alertNotifyTemplate.setTarget(target);
        alertNotifyTemplate.setTrigger(trigger);
        alertNotifyTemplate.setTitle(title);
        alertNotifyTemplate.setTemplate(template);
        alertNotifyTemplate.setFormats(formats);
        alertNotifyTemplate.setLanguage(language);
        alertNotifyTemplate.setVariable(variable);
        alertNotifyTemplate.setEnable(enable);
        return alertNotifyTemplate;
    }
}
