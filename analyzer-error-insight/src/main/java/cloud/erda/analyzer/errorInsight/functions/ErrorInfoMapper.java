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

import cloud.erda.analyzer.errorInsight.model.ErrorEvent;
import cloud.erda.analyzer.errorInsight.model.ErrorInfo;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * @author zhaihongwei
 * @since 2021/4/1
 */
public class ErrorInfoMapper implements MapFunction<ErrorEvent, ErrorInfo> {

    @Override
    public ErrorInfo map(ErrorEvent errorEvent) throws Exception {
        val errorInfo = new ErrorInfo();
        errorInfo.setTimestamp(errorEvent.getTimestamp());
        errorInfo.setErrorId(errorEvent.getErrorId());
        val eventTags = errorEvent.getTags();
        errorInfo.setServiceName(eventTags.getOrDefault(ErrorConstants.SERVICE_NAME, ""));
        errorInfo.setServiceId(eventTags.getOrDefault(ErrorConstants.SERVICE_ID, ""));
        errorInfo.setTerminusKey(eventTags.getOrDefault(ErrorConstants.TERMINUS_KEY, ""));
        errorInfo.setApplicationId(eventTags.getOrDefault(ErrorConstants.APPLICATION_ID, ""));
        errorInfo.setRuntimeName(eventTags.getOrDefault(ErrorConstants.RUNTIME_NAME, ""));
        val tags = new HashMap<String, String>();
        tags.putAll(errorEvent.getMetaData());
        tags.putAll(eventTags);
        tags.put(ErrorConstants.HTTP_PATH, errorEvent.getRequestContext().getOrDefault(ErrorConstants.HTTP_PATH, ""));
        errorInfo.setTags(tags);
        return errorInfo;
    }
}