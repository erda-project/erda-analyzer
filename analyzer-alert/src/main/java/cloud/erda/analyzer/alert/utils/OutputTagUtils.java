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

import cloud.erda.analyzer.alert.models.AlertEventNotifyMetric;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class OutputTagUtils {
    public static final OutputTag<AlertEventNotifyMetric> AlertEventNotifyProcess =
            new OutputTag<AlertEventNotifyMetric>("output-alert-event-processing-metric", TypeInformation.of(AlertEventNotifyMetric.class));
}
