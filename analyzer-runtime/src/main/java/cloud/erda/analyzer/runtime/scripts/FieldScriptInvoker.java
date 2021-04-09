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

import cloud.erda.analyzer.common.constant.Constants;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import lombok.extern.slf4j.Slf4j;
import javax.script.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: liuhaoyang
 * @create: 2019-07-03 09:07
 **/
@Slf4j
public class FieldScriptInvoker implements ScriptInvoker {

    private Invocable invocable;

    public FieldScriptInvoker(String script) {
        try {
            checkNotNull(script, "script");
            invocable = createScriptEngine(script);
        } catch (Throwable throwable) {
            log.warn(String.format("Create scriptEngine fail. The script [%s] will be ignored.", script), throwable);
        }
    }

    public Object invoke(Object... args) throws NoSuchMethodException, ScriptException {
        if (invocable == null) {
            return null;
        }
        return invocable.invokeFunction(Constants.INVOKE_FUNCTION_NAME, args);
    }

    private Invocable createScriptEngine(String script) throws ScriptException {
        NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
        ScriptEngine engine = factory.getScriptEngine(classFilter);
        CompiledScript compiledScript = ((Compilable) engine).compile(script);
        compiledScript.eval();
        return (Invocable) engine;
    }

    private static final ClassFilter classFilter  = new ClassFilter() {
        final Set<String> classSet = Collections.unmodifiableSet(new HashSet<String>() {{
            add("java.lang.Exception");
            add("java.lang.RuntimeException");
            add("java.lang.Object");
            add("java.lang.String");
            add("java.lang.Boolean");
            add("java.lang.Integer");
            add("java.lang.Long");
            add("java.lang.Double");
            add("java.lang.Float");
            add("java.lang.Math");
            add("java.util.Map");
            add("java.util.HashMap");
        }});

        @Override
        public boolean exposeToScripts(String className) {
            return classSet.contains(className);
        }
    };
}
