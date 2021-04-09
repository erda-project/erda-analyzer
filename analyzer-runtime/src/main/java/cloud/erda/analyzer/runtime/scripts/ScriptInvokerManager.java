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

package cloud.erda.analyzer.runtime.scripts;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2019-07-03 09:41
 **/
public class ScriptInvokerManager {

    private static final Map<String, ScriptInvoker> fieldInvokers = new HashMap<>();
    private static final Map<String, ScriptInvoker> valueInvokers = new HashMap<>();

    public static ScriptInvoker getFieldInvoker(String key, String script) {
        return fieldInvokers.computeIfAbsent(key, i -> new FieldScriptInvoker(script));
    }

    public static ScriptInvoker getValueInvoker(String key, String script) {
        return valueInvokers.computeIfAbsent(key, i -> new FieldScriptInvoker(script));
    }

}
