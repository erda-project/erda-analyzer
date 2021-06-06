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
import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.errorInsight.model.ErrorCountState;
import cloud.erda.analyzer.errorInsight.model.ErrorInfo;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * @author: liuhaoyang
 * @create: 2018-12-28 10:09
 **/
public class ErrorAlertMapper implements MapFunction<ErrorInfo, MetricEvent> {

    @Override
    public MetricEvent map(ErrorInfo errorInfo) throws Exception {
        HashMap<String, String> tags = new HashMap<>(errorInfo.getTags());
        tags.put(ErrorConstants.WORKSPACE, errorInfo.getTags().get(ErrorConstants.WORKSPACE));
        tags.put(ErrorConstants.PROJECT_ID, errorInfo.getTags().get(ErrorConstants.PROJECT_ID));
        tags.put(ErrorConstants.ERROR_ID, errorInfo.getErrorId());
        tags.put(ErrorConstants.TERMINUS_KEY, errorInfo.getTerminusKey());
        tags.put(ErrorConstants.SERVICE_NAME, errorInfo.getServiceName());
        String serviceId = StringUtil.isEmpty(errorInfo.getServiceId()) ? spliceServiceId(errorInfo) : errorInfo.getServiceId();
        tags.put(ErrorConstants.SERVICE_ID, serviceId);
        tags.put(Constants.META, String.valueOf(true));
        tags.put(Constants.SCOPE, Constants.MICRO_SERVICE);
        tags.put(Constants.SCOPE_ID, errorInfo.getTerminusKey());

        HashMap<String, Object> fields = new HashMap<>();
        fields.put(ErrorConstants.COUNT, 1);
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setName(ErrorConstants.ERROR_ALERT_NAME);
        metricEvent.setTimestamp(errorInfo.getTimestamp());
        metricEvent.setTags(tags);
        metricEvent.setFields(fields);
        System.out.println("------------"+metricEvent.getTags().get("service_id"));
        return metricEvent;
    }

    public String spliceServiceId(ErrorInfo errorInfo) {
        if (StringUtil.isEmpty(errorInfo.getApplicationId())) {
            if (StringUtil.isEmpty(errorInfo.getRuntimeName())) {
                return errorInfo.getServiceName();
            }
            return errorInfo.getRuntimeName() + "_" + errorInfo.getServiceName();
        }
        return errorInfo.getApplicationId() + "_" + errorInfo.getRuntimeName() + "_" + errorInfo.getServiceName();
    }

}
