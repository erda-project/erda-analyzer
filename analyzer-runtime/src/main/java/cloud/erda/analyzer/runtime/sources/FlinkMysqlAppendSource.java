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

import java.sql.ResultSet;
import java.util.Properties;

/**
 * @author: liuhaoyang
 * @create: 2019-06-28 16:31
 **/
public class FlinkMysqlAppendSource<T> extends FlinkMysqlSource<T> {


    public FlinkMysqlAppendSource(String querySql, long interval, DataRowReader<T> rowReader, Properties properties) {
        super(querySql, interval, rowReader, properties);
    }

    @Override
    protected void processResultSet(ResultSet resultSet, SourceContext<T> ctx) throws Exception {
        while (resultSet.next()) {
            T read = dataRowReader.read(resultSet);
            if (read != null) {
                ctx.collect(read);
            }
        }
    }
}
