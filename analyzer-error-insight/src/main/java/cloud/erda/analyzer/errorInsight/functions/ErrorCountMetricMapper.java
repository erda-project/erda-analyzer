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

package cloud.erda.analyzer.errorInsight.functions;

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.errorInsight.model.ErrorCountState;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * @author: liuhaoyang
 * @create: 2018-12-25 10:55
 **/
public class ErrorCountMetricMapper implements MapFunction<ErrorCountState, MetricEvent> {

    @Override
    public MetricEvent map(ErrorCountState errorCountState) throws Exception {
        HashMap<String, String> tags = new HashMap<>(4);
        tags.put(ErrorConstants.WORKSPACE, errorCountState.getWorkspace());
        tags.put(ErrorConstants.PROJECT_ID, errorCountState.getProjectId());
        tags.put(ErrorConstants.PROJECT_NAME, errorCountState.getProjectName());
        tags.put(ErrorConstants.APPLICATION_ID, errorCountState.getApplicationId());
        tags.put(ErrorConstants.APPLICATION_NAME, errorCountState.getApplicationName());
        tags.put(ErrorConstants.CLUSTER_NAME, errorCountState.getClusterName());
        tags.put(ErrorConstants.TERMINUS_KEY, errorCountState.getTerminusKey());
        tags.put(ErrorConstants.SERVICE_NAME, errorCountState.getServiceName());
        String serviceId = errorCountState.getServiceId() == null ? spliceServiceId(errorCountState) : errorCountState.getServiceId();
        tags.put(ErrorConstants.SERVICE_ID, serviceId);
        tags.put(ErrorConstants.RUNTIME_NAME, errorCountState.getRuntimeName());
        tags.put(ErrorConstants.RUNTIME_ID, errorCountState.getRuntimeId());
        tags.put(ErrorConstants.EXCEPTION, errorCountState.getException());
        tags.put(ErrorConstants.ORG_NAME, errorCountState.getOrgName());
        tags.put(Constants.META, String.valueOf(true));
        tags.put(Constants.SCOPE, Constants.MICRO_SERVICE);
        tags.put(Constants.SCOPE_ID, errorCountState.getTerminusKey());

        HashMap<String, Object> fields = new HashMap<>(1);
        fields.put(ErrorConstants.COUNT, errorCountState.getCount());

        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setName(ErrorConstants.ERROR_COUNT_NAME);
        metricEvent.setTimestamp(errorCountState.getTimestamp());
        metricEvent.setTags(tags);
        metricEvent.setFields(fields);
        return metricEvent;
    }

    public String spliceServiceId(ErrorCountState errorCountState) {
        if (errorCountState.getApplicationId() == null || errorCountState.getApplicationId().equals("")) {
            if (errorCountState.getRuntimeName() == null || errorCountState.getRuntimeName().equals("")) {
                return errorCountState.getServiceName();
            }
            return errorCountState.getRuntimeName() + "_" + errorCountState.getServiceName();
        }
        return errorCountState.getApplicationId() + "_" + errorCountState.getRuntimeName() + "_" + errorCountState.getServiceName();
    }
}
