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

package cloud.erda.analyzer.alert.utils;

import cloud.erda.analyzer.alert.models.AlertNotify;
import cloud.erda.analyzer.alert.models.AlertNotifyTemplate;
import cloud.erda.analyzer.alert.models.Notify;
import cloud.erda.analyzer.alert.models.UniversalTemplate;
import cloud.erda.analyzer.alert.models.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.util.Map;

import static cloud.erda.analyzer.common.constant.MetricConstants.ALERT_NOTIFY_STATE;
import static cloud.erda.analyzer.common.constant.MetricConstants.ALERT_NOTIFY_TEMPLATE_STATE;

public class StateDescriptors {

    public static final MapStateDescriptor<String, Map<Long, AlertNotify>> alertNotifyStateDescriptor =
            new MapStateDescriptor<>(ALERT_NOTIFY_STATE, BasicTypeInfo.STRING_TYPE_INFO,
                    new MapTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(AlertNotify.class)));

    public static final MapStateDescriptor<String, AlertNotifyTemplate> alertNotifyTemplateStateDescriptor =
            new MapStateDescriptor<>(ALERT_NOTIFY_TEMPLATE_STATE, BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(AlertNotifyTemplate.class));

    public static final MapStateDescriptor<Long, Notify> notifyStateDescriptor =
            new MapStateDescriptor<>("notify_state", BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(Notify.class));
    public static final MapStateDescriptor<String, Map<String, UniversalTemplate>> notifyTemplate =
            new MapStateDescriptor<>("notify_template_state", BasicTypeInfo.STRING_TYPE_INFO,
                    new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(UniversalTemplate.class)));
    public static final MapStateDescriptor<Long,String> diceOrgDescriptor =
            new MapStateDescriptor<>("dice_org", BasicTypeInfo.LONG_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO);
}
