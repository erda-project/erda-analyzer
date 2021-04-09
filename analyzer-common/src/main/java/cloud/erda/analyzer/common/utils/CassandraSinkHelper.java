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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.util.List;

@Slf4j
public class CassandraSinkHelper {

    public static boolean execute(@NotNull Session session, List<Statement> statements, boolean async, int batchSize) {
        if (statements.isEmpty()) {
            return true;
        }
        List<List<Statement>> smallerLists = Lists.partition(statements, batchSize);
        for (List<Statement> statements1 : smallerLists) {
            boolean result = execute(session, statements1, async);
            if (!result) {
                return false;
            }
        }
        return true;
    }

    public static boolean execute(@NotNull Session session, List<Statement> statements, boolean async) {
        if (statements.isEmpty()) {
            return true;
        }
        long now = System.currentTimeMillis();
        try {
            if (async) {
                BatchStatement batchStatement = new BatchStatement();
                batchStatement.addAll(statements);
                session.executeAsync(batchStatement);
            } else {
                executeSync(session, statements);
            }
            log.debug("Execute insert  success, size: {}, cost: {}", statements.size(), System.currentTimeMillis() - now);
            return true;
        } catch (Exception e) {
            log.error("Execute insert error, err: {}", Throwables.getStackTraceAsString(e));
        }
        return false;

    }

    public static void executeSync(Session session, List<Statement> statementList) {
        BatchStatement batchStatement = new BatchStatement();
        try {
            if (!statementList.isEmpty()) {
                session.execute(batchStatement.addAll(statementList));
            }
        } catch (com.datastax.driver.core.exceptions.InvalidQueryException e) {
            if (e.getMessage().equals("Batch too large")) {
                if (statementList.size() == 1) {
                    throw e;
                }
                List<Statement> part1 = statementList.subList(0, statementList.size() / 2);
                List<Statement> part2 = statementList.subList(statementList.size() / 2, statementList.size());
                executeSync(session, part1);
                executeSync(session, part2);
            } else {
                throw e;
            }
        }
    }


}
