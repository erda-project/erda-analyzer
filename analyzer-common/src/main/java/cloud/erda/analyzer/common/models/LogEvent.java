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

package cloud.erda.analyzer.common.models;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class LogEvent {

    // sources of { deployment, container or business id }
    private String source;

    // Log id
    private String id;

    // timestamp of log
    private Long timestamp;

    // stdout or stderr
    private String stream;

    // The offset of logfile
    private Long offset;

    // offset content
    private String content;

    // Tags of { docker, system or business tag }
    private Map<String, String> tags = new HashMap<>();
}
