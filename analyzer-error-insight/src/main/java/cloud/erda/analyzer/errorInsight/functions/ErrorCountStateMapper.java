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

import cloud.erda.analyzer.errorInsight.model.ErrorCountState;
import cloud.erda.analyzer.errorInsight.model.ErrorInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 18:46
 **/
public class ErrorCountStateMapper implements MapFunction<ErrorInfo, ErrorCountState> {

    @Override
    public ErrorCountState map(ErrorInfo error) throws Exception {
        ErrorCountState count = new ErrorCountState();
        count.setCount(1);
        count.setProjectId(error.getTags().getOrDefault(ErrorConstants.PROJECT_ID, ""));
        count.setProjectName(error.getTags().getOrDefault(ErrorConstants.PROJECT_NAME, StringUtils.EMPTY));
        count.setTerminusKey(error.getTerminusKey());
        count.setServiceName(error.getServiceName());
        count.setServiceId(error.getServiceId());
        count.setApplicationId(error.getApplicationId());
        count.setApplicationName(error.getTags().getOrDefault(ErrorConstants.APPLICATION_NAME, StringUtils.EMPTY));
        count.setWorkspace(error.getTags().getOrDefault(ErrorConstants.WORKSPACE, ""));
        count.setErrorId(error.getErrorId());
        count.setException(error.getTags().getOrDefault(ErrorConstants.ERROR_TYPE, StringUtils.EMPTY));
        count.setTimestamp(error.getTimestamp());
        count.setClusterName(error.getTags().get(ErrorConstants.CLUSTER_NAME));
        count.setRuntimeName(error.getTags().get(ErrorConstants.RUNTIME_NAME));
        count.setOrgName(error.getTags().get(ErrorConstants.ORG_NAME));
        count.setRuntimeId(error.getTags().get(ErrorConstants.RUNTIME_ID));
        return count;
    }
}
