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

package cloud.erda.analyzer.tracing.model;

import lombok.Data;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/9/17 17:00
 */
@Data
public class Span {

    private String traceID;

    private String spanID;

    private String parentSpanID;

    private Long startTimeUnixNano;

    private Long endTimeUnixNano;

    private String name;

    private Relation relations;

    private Map<String, String> attributes;
}