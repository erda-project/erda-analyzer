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

package cloud.erda.analyzer.runtime.models;

import cloud.erda.analyzer.runtime.JsonDeserializer.ArrayToStringDeserializer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;

import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2019-06-28 14:34
 **/
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExpressionMetadata {

    private String id;

    private String version;
    @JsonDeserialize(contentUsing = ArrayToStringDeserializer.class)
    private Map<String, String> attributes;

    private Expression expression;

    private boolean enable;

    private long processingTime;
}


