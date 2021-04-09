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

import cloud.erda.analyzer.errorInsight.model.ErrorDescription;
import cloud.erda.analyzer.errorInsight.model.ErrorInfo;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 18:28
 **/
public class ErrorDescriptionMapper implements MapFunction<ErrorInfo, ErrorDescription> {

    @Override
    public ErrorDescription map(ErrorInfo errorInfo) throws Exception {
        val description = new ErrorDescription();
        description.setApplicationId(errorInfo.getApplicationId());
        description.setErrorId(errorInfo.getErrorId());
        description.setServiceName(errorInfo.getServiceName());
        description.setTags(errorInfo.getTags());
        description.setTerminusKey(errorInfo.getTerminusKey());
        description.setTimestamp(errorInfo.getTimestamp());
        return description;
    }
}
