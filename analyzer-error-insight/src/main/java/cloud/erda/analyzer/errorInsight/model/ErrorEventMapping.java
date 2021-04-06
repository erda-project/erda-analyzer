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

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import cloud.erda.analyzer.common.utils.annotations.ClusteringOrder;
import lombok.Data;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 14:55
 **/
@Data
@Table(keyspace = "spot_prod", name = "error_event_mapping")
public class ErrorEventMapping {

    @PartitionKey(0)
    @Column(name = "error_id")
    private String errorId;

    @ClusteringColumn
    @ClusteringOrder(ClusteringOrder.Order.DESC)
    @Column(name = "timestamp")
    private long timestamp;

    @Column(name = "event_id")
    private String eventId;
}
