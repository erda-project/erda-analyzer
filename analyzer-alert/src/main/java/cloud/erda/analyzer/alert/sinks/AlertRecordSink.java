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

package cloud.erda.analyzer.alert.sinks;

import cloud.erda.analyzer.alert.models.AlertRecord;
import cloud.erda.analyzer.common.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author randomnil
 */
@Slf4j
public class AlertRecordSink extends DBPoolSink<AlertRecord> {

    private static final String PREPARE_STATEMENT = "INSERT INTO `sp_alert_record`"
            + "(`group_id`, `scope`, `scope_key`, `alert_group`, `title`, `alert_state`, `alert_type`, `alert_index`, "
            + "`expression_key`, `alert_id`, `alert_name`, `rule_id`, `alert_time`) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE "
            + "`alert_state` = ?, `alert_type` = ?, `alert_index` = ?, `alert_name` = ?, `alert_time` = ?";

    private String url;
    private String user;
    private String password;
    private Connection conn;
    private PreparedStatement ps;

    private long interval = 5000;
    private long lastExecTimestamp;
    private int batchSize = 200;
    private List<AlertRecord> queue;

    public AlertRecordSink(Properties properties) {
        this.url = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false",
                properties.getProperty(Constants.MYSQL_HOST),
                properties.getProperty(Constants.MYSQL_PORT),
                properties.getProperty(Constants.MYSQL_DATABASE));
        this.user = properties.getProperty(Constants.MYSQL_USERNAME);
        this.password = properties.getProperty(Constants.MYSQL_PASSWORD);

        val batchSize = properties.getProperty(Constants.MYSQL_BATCH_SIZE);
        if (StringUtils.isNotBlank(batchSize)) {
            this.batchSize = Integer.valueOf(batchSize);
        }
        val interval = properties.getProperty(Constants.MYSQL_INTERVAL);
        if (StringUtils.isNotBlank(interval)) {
            this.interval = Integer.valueOf(interval);
        }
        this.queue = new ArrayList<>(this.batchSize);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.openConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.execute();
       this.conn.close();
    }

    @Override
    public void invoke(AlertRecord value, Context context) throws Exception {
        this.queue.add(value);
        if (this.queue.size() > this.batchSize
                || System.currentTimeMillis() - this.lastExecTimestamp > this.interval) {
            execute();
            this.lastExecTimestamp = System.currentTimeMillis();
        }
    }

    private void openConnection() throws SQLException, PropertyVetoException {
        initConnection(this.url,this.user,this.password);
        this.conn = newConnection();
        this.ps = conn.prepareStatement(PREPARE_STATEMENT);
        this.lastExecTimestamp = System.currentTimeMillis();
    }

    private void execute() throws Exception {
        for (val value : this.queue) {
            int i = 1;
            ps.setString(i++, value.getGroupId());
            ps.setString(i++, value.getScope());
            ps.setString(i++, value.getScopeKey());
            ps.setString(i++, value.getAlertGroup());
            ps.setString(i++, value.getTitle());
            ps.setString(i++, value.getAlertState());
            ps.setString(i++, value.getAlertType());
            ps.setString(i++, value.getAlertIndex());
            ps.setString(i++, value.getExpressionKey());
            ps.setLong(i++, value.getAlertId());
            ps.setString(i++, value.getAlertName());
            ps.setLong(i++, value.getRuleId());
            ps.setTimestamp(i++, new Timestamp(value.getAlertTime()));
            ps.setString(i++, value.getAlertState());
            ps.setString(i++, value.getAlertType());
            ps.setString(i++, value.getAlertIndex());
            ps.setString(i++, value.getAlertName());
            ps.setTimestamp(i++, new Timestamp(value.getAlertTime()));
            ps.addBatch();
        }
        try {
            val result = ps.executeBatch();
            val s = new StringBuffer();
            for (val item : result) {
                s.append(item).append(",");
            }
            log.info("mysql sink invoke. result: {}", s);
        } catch (Exception e) {
            log.error("mysql sink invoke err.", e);
        }
        queue.clear();
    }
}