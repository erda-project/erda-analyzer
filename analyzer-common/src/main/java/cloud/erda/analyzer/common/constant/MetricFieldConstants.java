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

package cloud.erda.analyzer.common.constant;


public class MetricFieldConstants {


    //name=diskio,procstat
    public static final String WRITE_BYTES = "write_bytes";
    public static final String READ_BYTES = "read_bytes";


    //name=diskio
    public static final String WRITE_TIME = "write_time";
    public static final String READ_TIME = "read_time";
    public static final String WEIGHTED_IO_TIME = "weighted_io_time";
    public static final String READS = "reads";
    public static final String READ_RATE = "read_rate";
    public static final String WRITES = "writes";
    public static final String WRITE_RATE = "write_rate";

    //name=procstat
    public static final String READ_COUNT = "read_count";
    public static final String WRITE_COUNT = "write_count";
    public static final String PROCSTAT_CPU_RANK = "cpu_usage_topk_rank";
    public static final String PROCSTAT_READ_RANK = "read_rate_topk_rank";
    public static final String PROCSTAT_READ_RATE = "read_rate";
    public static final String PROCSTAT_WRITE_RANK = "write_rate_topk_rank";
    public static final String PROCSTAT_WRITE_RATE = "write_rate";

    //status-page metric fields
    public static final String CODE = "code";
    public static final String RETRY = "retry";
    public static final String LATENCY = "latency";
    public static final String MESSAGE = "message";
    public static final String REQUEST_ID = "request_id";
    public static final String DIFF_SCREENSHOTS = "diff_screenshots";
    public static final String CUR_SCREENSHOTS = "cur_screenshots";
    public static final String PRE_SCREENSHOTS = "pre_screenshots";


    //    public static final String SPEED_READ_BYTES = "speed_read_bytes";
    public static final String SPEED_READ_COUNT = "speed_read_count";
    public static final String SPEED_WRITE_BYTES = "speed_write_bytes";
    public static final String SPEED_WRITE_COUNT = "speed_write_count";
    public static final String CPU_USEAGE_ACTIVE = "usage_active";
    public static final String MEM_USED_PERCENT = "used_percent";

    public static final String CPU_USAGE = "cpu_usage";

    //name=disk
    public static final String DISK_USED_PERCENT = "used_percent";
    public static final String DISK_PCT_UTIL = "pct_util";

    //name=system
    public static final String SYSTEM_LOAD1 = "load1";
    public static final String SYSTEM_LOAD5 = "load5";
    public static final String SYSTEM_LOAD15 = "load15";

    //name=netstat
    public static final String NETSTAT_TCP_CLOSE = "tcp_close";

    public static final String NETSTAT_TCP_TIME_WAIT = "tcp_time_wait";
    public static final String NETSTAT_TCP_SYN_SENT = "tcp_syn_sent";//tcp_syn_recv
    public static final String NETSTAT_TCP_SYN_RECV = "tcp_syn_recv";
    //swap
    public static final String SWAP_USED_PENCENT = "used_percent";


    //name=cpu
    public static final String CPU_USAGE_IDLE = "usage_idle";

    //processes
    public static final String BLOCKED = "blocked";
    public static final String RUNNING = "running";

    //addon
    public static final String ADDON_STATUS = "instance_status";

    //error
    public static final String COUNT = "count";

    //基础组件的fields常量
    public static final String USAGE_PERCENT = "usage_percent";
    public static final String LIMIT = "limit";
    public static final String USAGE = "usage";

    public static final String STATUS = "status";
    public static final String READY = "ready";
    public static final String NOT_READY = "not_ready";

    public static final String LAST_TIMESTAMP = "last_timestamp";

}
