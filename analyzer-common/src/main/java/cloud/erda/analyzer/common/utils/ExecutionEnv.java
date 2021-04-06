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

import cloud.erda.analyzer.common.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ExecutionEnv {
    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool
                .fromPropertiesFile(ExecutionEnv.class.getResourceAsStream("/application.properties"))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnv.class.getResourceAsStream("/application.properties"))
                    .mergeWith(ParameterTool.fromSystemProperties())
                    .mergeWith(ParameterTool.fromMap(getenv()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(Constants.FLINK_PARALLELISM_DEFAULT, 1));
        if (parameterTool.getBoolean(Constants.STREAM_CHECKPOINT_ENABLE, false)) {
            env.enableCheckpointing(parameterTool.getInt(Constants.STREAM_CHECKPOINT_INTERVAL, 5000), CheckpointingMode.EXACTLY_ONCE); // create a checkpoint every 5 seconds
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            String stateBackend = parameterTool.get(Constants.STREAM_STATE_BACKEND, Constants.STATE_BACKEND_MEMORY);
            String fsStateBackendPath = parameterTool.get(Constants.STREAM_FS_STATE_BACKEND_PATH, "file:///tmp/checkpoints");
            if (stateBackend.equals(Constants.STATE_BACKEND_MEMORY)) {
                env.setStateBackend(new MemoryStateBackend(5 * 1024 * 1024 * 100));
            }
            if (stateBackend.equals(Constants.STATE_BACKEND_FS)) {
                env.setStateBackend(new FsStateBackend(new URI(fsStateBackendPath), 0));
            }
            if (stateBackend.equals(Constants.STATE_BACKEND_ROCKSDB)) {
                RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new URI(fsStateBackendPath))
                        .configure(parameterTool.getConfiguration(), ClassLoader.getSystemClassLoader());
                log.info(rocksDBStateBackend.toString());
                env.setStateBackend(rocksDBStateBackend);
            }
        }
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.minutes(1)));
        return env;
    }

    private static Map<String, String> getenv() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey().toLowerCase().replace('_', '.'), entry.getValue());
        }
        return map;
    }
}
