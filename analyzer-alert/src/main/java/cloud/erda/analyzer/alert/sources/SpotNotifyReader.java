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

package cloud.erda.analyzer.alert.sources;

import cloud.erda.analyzer.alert.models.NotifyTarget;
import cloud.erda.analyzer.alert.models.Notify;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.sources.DataRowReader;

import java.sql.ResultSet;
import java.util.ArrayList;

public class SpotNotifyReader implements DataRowReader<Notify> {

    @Override
    public Notify read(ResultSet resultSet) throws Exception {
        Notify notify = new Notify();
        Long id = resultSet.getLong("id");
        String target = resultSet.getString("target");
        NotifyTarget notifyTarget = JsonMapperUtils.toObject(target, NotifyTarget.class);
        String templateIds = resultSet.getString("notify_id");
        ArrayList<String> templateIDArr = JsonMapperUtils.toArrayList(templateIds, String.class);
        String scopeType = resultSet.getString("scope");
        String scopeId = resultSet.getString("scope_id");
        String attribute = resultSet.getString("attributes");
        String notifyName = resultSet.getString("notify_name");
        Long processTime = System.currentTimeMillis();
        notify.setId(id);
        notify.setProcessTime(processTime);
        notify.setTarget(notifyTarget);
        notify.setTemplateIds(templateIDArr.toArray(new String[0]));
        notify.setScope(scopeType);
        notify.setScopeId(scopeId);
        notify.setAttribute(attribute);
        notify.setNotifyName(notifyName);
        return notify;
    }
}
