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
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 18:50
 **/
public class ErrorCountStateAggregator implements ReduceFunction<ErrorCountState> {

    @Override
    public ErrorCountState reduce(ErrorCountState first, ErrorCountState append) throws Exception {
        first.setCount(first.getCount() + append.getCount());
        return first;
    }
}
