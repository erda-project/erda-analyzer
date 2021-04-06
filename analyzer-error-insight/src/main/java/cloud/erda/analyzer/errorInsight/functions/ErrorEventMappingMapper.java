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
import cloud.erda.analyzer.errorInsight.model.ErrorEventMapping;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 18:17
 **/
public class ErrorEventMappingMapper implements MapFunction<ErrorEvent, ErrorEventMapping> {

    @Override
    public ErrorEventMapping map(ErrorEvent errorEvent) throws Exception {
        ErrorEventMapping mapping = new ErrorEventMapping();
        mapping.setEventId(errorEvent.getEventId());
        mapping.setErrorId(errorEvent.getErrorId());
        mapping.setTimestamp(errorEvent.getTimestamp());
        return mapping;
    }
}
