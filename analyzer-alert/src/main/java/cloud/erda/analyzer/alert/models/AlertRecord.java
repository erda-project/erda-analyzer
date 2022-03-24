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

import lombok.Data;

/**
 * @author randomnil
 */
@Data
public class AlertRecord {

    private String groupId;

    private String scope;

    private String scopeKey;

    private String alertGroup;

    private String title;

    private String alertState;

    private String alertType;

    private String alertIndex;

    private String expressionKey;

    private Long alertId;

    private String alertName;

    private String ruleId;

    private String ruleName;

    private Long alertTime;

    private String alertEventFamilyId;

    private String alertSource;

    private String alertSubject;

    private String alertTriggerFunctions;

    private String alertLevel;

    private Long orgId;
}
