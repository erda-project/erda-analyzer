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

package cloud.erda.analyzer.errorInsight.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 14:17
 **/
@Data
@Table(keyspace = "spot_prod", name = "error_events")
public class ErrorEvent {
    @PartitionKey
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "timestamp")
    private long timestamp;

    @Column(name = "request_id")
    private String requestId;

    @Column(name = "error_id")
    private String errorId;

    @Column(name = "stacks")
    private List<String> stacks;

    @Column(name = "tags")
    private Map<String, String> tags;

    @Column(name = "meta_data")
    private Map<String, String> metaData;

    @Column(name = "request_context")
    private Map<String, String> requestContext;

    @Column(name = "request_headers")
    private Map<String, String> requestHeaders;
}
