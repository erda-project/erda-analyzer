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

package cloud.erda.analyzer.alert.partitioners;

import cloud.erda.analyzer.alert.models.AlertRecord;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

public class AlertRecordKafkaPartitioner extends FlinkKafkaPartitioner<AlertRecord> {

    @Override
    public int partition(AlertRecord entity, byte[] bytes, byte[] bytes1, String topic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");
        return partitions[(entity.getAlertEventFamilyId().hashCode() & Integer.MAX_VALUE) % partitions.length];
    }
}
