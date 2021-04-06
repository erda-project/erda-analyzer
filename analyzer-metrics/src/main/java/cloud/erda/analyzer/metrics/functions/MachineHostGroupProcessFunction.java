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

package cloud.erda.analyzer.metrics.functions;

import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @author: liuhaoyang
 * @create: 2019-12-26 14:33
 **/
@Slf4j
public class MachineHostGroupProcessFunction implements KeySelector<MetricEvent, String> {

    @Override
    public String getKey(MetricEvent value) throws Exception {
        return value.getTags().get(MetricTagConstants.ORG_NAME) + value.getTags().get(MetricTagConstants.CLUSTER_NAME) + value.getTags().get(MetricTagConstants.HOSTIP);
    }
}
