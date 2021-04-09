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

import cloud.erda.analyzer.errorInsight.model.ErrorCount;
import cloud.erda.analyzer.errorInsight.model.ErrorCountState;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: liuhaoyang
 * @create: 2018-12-25 10:52
 **/
public class ErrorCountMapper implements MapFunction<ErrorCountState, ErrorCount> {

    @Override
    public ErrorCount map(ErrorCountState errorCountState) throws Exception {
        ErrorCount count = new ErrorCount();
        count.setCount(errorCountState.getCount());
        count.setTimestamp(errorCountState.getTimestamp());
        count.setErrorId(errorCountState.getErrorId());
        return count;
    }
}
