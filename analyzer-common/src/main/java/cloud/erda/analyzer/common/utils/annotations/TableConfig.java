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

package cloud.erda.analyzer.common.utils.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TableConfig {


    double bloomFilterFpChance() default 0.01;

    double crcCheckChance() default 1.0;

    double dclocalReadRepairChance() default 0.1;

    int defaultTimeToLive() default 0;

    int gcGraceSeconds() default 864000;

    int maxIndexInterval() default 2048;

    int memtableFlushPeriodInMs() default 0;

    int minIndexInterval() default 128;

    double readRepairChance() default 0.0;

    String speculativeRetry() default "99PERCENTILE";

    String comment() default "";

//   AND bloom_filter_fp_chance = 0.01
//    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
//    AND comment = ''
//    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
//    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
//    AND crc_check_chance = 1.0
//    AND dclocal_read_repair_chance = 0.1
//    AND default_time_to_live = 0
//    AND gc_grace_seconds = 864000
//    AND max_index_interval = 2048
//    AND memtable_flush_period_in_ms = 0
//    AND min_index_interval = 128
//    AND read_repair_chance = 0.0
//    AND speculative_retry = '99PERCENTILE';

}
