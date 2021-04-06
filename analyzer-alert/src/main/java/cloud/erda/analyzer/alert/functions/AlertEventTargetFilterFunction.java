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

import cloud.erda.analyzer.alert.models.AlertEvent;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author: liuhaoyang
 * @create: 2020-01-06 16:33
 **/
public class AlertEventTargetFilterFunction implements FilterFunction<AlertEvent> {

    private final HashSet targets;

    public AlertEventTargetFilterFunction(String... targets) {
        this.targets = new HashSet();
        if (targets != null && targets.length > 0) {
            this.targets.addAll(Arrays.asList(targets));
        }
    }

    @Override
    public boolean filter(AlertEvent value) throws Exception {
        return targets.contains(value.getAlertNotify().getNotifyTarget().getType());
    }
}
