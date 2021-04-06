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

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import cloud.erda.analyzer.common.utils.annotations.ClusteringOrder;
import lombok.Data;

import java.util.Map;

/**
 * @author: liuhaoyang
 * @update: 2018-12-24
 **/

/**
 * App-Insight 和 Trace-Insight都用到，同时也是cassandra model
 */
@Data
@Table(keyspace = "spot_prod", name = "spans")
public class SpanEvent {

    @PartitionKey
    @Column(name = "trace_id")
    private String traceId;

    @ClusteringColumn
    @ClusteringOrder(ClusteringOrder.Order.DESC)
    @Column(name = "start_time")
    private long startTime;

    @Column(name = "span_id")
    private String spanId;

    @Column(name = "parent_span_id")
    private String parentSpanId;

    @Column(name = "operation_name")
    private String operationName;

    @Column(name = "end_time")
    private long endTime;

    @Column(name = "tags")
    private Map<String, String> tags;
}
