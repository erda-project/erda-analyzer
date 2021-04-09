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

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class DBPoolSink<IN> extends RichSinkFunction<IN> {

    private ComboPooledDataSource comboPooledDataSource;
    private final String DRIVER = "com.mysql.cj.jdbc.Driver";

    public void initConnection(String url, String user, String password) throws PropertyVetoException {
        this.comboPooledDataSource = new ComboPooledDataSource();
        this.comboPooledDataSource.setDriverClass(DRIVER);
        this.comboPooledDataSource.setJdbcUrl(url);
        this.comboPooledDataSource.setUser(user);
        this.comboPooledDataSource.setPassword(password);
        this.comboPooledDataSource.setTestConnectionOnCheckin(true);
        //每2小时检查连接是否有效，如果断开则自动重连
        this.comboPooledDataSource.setIdleConnectionTestPeriod(1200);
    }

    public Connection newConnection() throws SQLException {
        return this.comboPooledDataSource.getConnection();
    }
}