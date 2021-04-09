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

import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 14:20
 **/
@Data
@Table(keyspace = "spot_prod", name = "error_description_v2")
public class ErrorDescription {

    @PartitionKey()
    @Column(name = "terminus_key")
    private String terminusKey;

    @PartitionKey(1)
    @Column(name = "application_id")
    private String applicationId;

    @PartitionKey(2)
    @Column(name = "service_name")
    private String serviceName;

    @PartitionKey(3)
    @Column(name = "error_id")
    private String errorId;

    @Column(name = "timestamp")
    private long timestamp;

    @Column(name = "tags")
    private Map<String, String> tags;
}
