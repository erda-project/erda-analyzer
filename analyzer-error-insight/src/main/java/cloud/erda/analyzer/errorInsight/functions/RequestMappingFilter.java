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
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 15:00
 **/
public class RequestMappingFilter implements FilterFunction<ErrorEvent> {

    @Override
    public boolean filter(ErrorEvent errorEvent) throws Exception {
        String requestId = errorEvent.getRequestId();
        return requestId != null && requestId.length() > 0;
    }
}
