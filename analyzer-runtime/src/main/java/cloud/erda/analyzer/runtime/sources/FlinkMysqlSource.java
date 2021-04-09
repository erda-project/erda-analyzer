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

package cloud.erda.analyzer.runtime.sources;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static cloud.erda.analyzer.common.constant.Constants.MYSQL_USERNAME;
import static cloud.erda.analyzer.common.constant.Constants.MYSQL_DATABASE;
import static cloud.erda.analyzer.common.constant.Constants.MYSQL_HOST;
import static cloud.erda.analyzer.common.constant.Constants.MYSQL_PORT;
import static cloud.erda.analyzer.common.constant.Constants.MYSQL_PASSWORD;

/**
 * @author: liuhaoyang
 * @create: 2019-06-28 10:16
 **/
@Slf4j
public abstract class FlinkMysqlSource<T> extends AbstractRichFunction implements SourceFunction<T> {

    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final long DEFAULT_INTERVAL = 60000;

    private volatile boolean isRunning = true;
    protected final DataRowReader<T> dataRowReader;
    protected final Properties properties;
    protected final String querySql;
    protected final String connectionString;
    protected final long queryInterval;
    private Connection connection;

    public FlinkMysqlSource(String querySql, long interval, DataRowReader<T> rowReader, Properties properties) {
        this.querySql = checkNotNull(querySql, "querySql");
        this.dataRowReader = rowReader; // checkNotNull(rowReader, "rowReader");
        this.properties = checkNotNull(properties, "properties");
        if (interval <= 0) {
            interval = DEFAULT_INTERVAL;
        }
        this.queryInterval = interval;
        this.connectionString = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8",
                properties.getProperty(MYSQL_HOST),
                properties.getProperty(MYSQL_PORT),
                properties.getProperty(MYSQL_DATABASE));
        log.info("init mysql source. connection = {} querySql = {} queryInterval = {}ms", connectionString, querySql, queryInterval);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        this.connection = getConnection();
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(querySql);
            while (isRunning) {
                ResultSet resultSet = statement.executeQuery();
                processResultSet(resultSet, ctx);
                TimeUnit.MILLISECONDS.sleep(queryInterval);
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (SQLException e) {
            // cancel 不需要把异常抛到上层，故只打印日志
            log.error("Close mysql connection error", e);
        }
    }

    protected abstract void processResultSet(ResultSet resultSet, SourceContext<T> ctx) throws Exception;

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER);
        Connection connection = DriverManager.getConnection(connectionString, properties.getProperty(MYSQL_USERNAME), properties.getProperty(MYSQL_PASSWORD));
        return connection;
    }
}
