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

package cloud.erda.analyzer.common.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.cassandra.CassandraCommitter;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CassandraBaseCommitter extends CassandraCommitter {
    private List<String> preSqls = new ArrayList<>();

    public CassandraBaseCommitter(ClusterBuilder builder) {
        super(builder);
        this.builder = builder;
    }

    private final ClusterBuilder builder;

    /**
     * Generates the necessary tables to store information.
     *
     * @throws Exception
     */
    @Override
    public void createResource() throws Exception {
        Cluster cluster = builder.getCluster();
        Session session = cluster.connect();
        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s with replication={'class':'SimpleStrategy', 'replication_factor':3};", "flink_auxiliary"));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (sink_id text, sub_id int, checkpoint_id bigint, PRIMARY KEY (sink_id, sub_id));", "flink_auxiliary", "checkpoints_" + super.jobId));
        for (String sql : preSqls) {
            try {
                session.execute(sql);
                Thread.sleep(500);//wait 500ms for 同步信息

            } catch (Exception e) {
                log.error("execute sql error {},{}", sql, e);
                throw new RuntimeException("execute  error, sql:" + sql, e);
            }
            log.info("execute sql success {}", sql);
        }


        close(cluster, session);
    }

    private void close(@NotNull Cluster cluster, @NotNull Session session) {
        try {
            session.close();
            cluster.close();
        } catch (Exception e) {
            LOG.error("Error while  session.", e);
        }
    }


    public void setPreSqls(List<String> preSqls) {
        this.preSqls = preSqls;
    }

    public List<String> getPreSqls() {
        return preSqls;
    }
}
