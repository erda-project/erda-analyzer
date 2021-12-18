package cloud.erda.analyzer.common.partitioners;

import cloud.erda.analyzer.common.models.Entity;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

public class EntityKafkaPartitioner extends FlinkKafkaPartitioner<Entity> {

    @Override
    public int partition(Entity entity, byte[] bytes, byte[] bytes1, String topic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");
        return partitions[(entity.getType().hashCode() ^ entity.getKey().hashCode()) % partitions.length];
    }
}
