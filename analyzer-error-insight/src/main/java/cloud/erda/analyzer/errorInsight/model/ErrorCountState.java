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

package cloud.erda.analyzer.errorInsight.model;

import lombok.Data;

/**
 * @author: liuhaoyang
 * @create: 2018-12-25 10:48
 **/
@Data
public class ErrorCountState {

    private String projectId;

    private String projectName;

    private String applicationId;

    private String applicationName;

    private String workspace;

    private String errorId;

    private String exception;

    private String clusterName;

    private String terminusKey;

    private String serviceId;

    private String serviceName;

    private String runtimeName;

    private String runtimeId;

    private String orgName;

    private long timestamp;

    private long count;

    public String getServiceId() {
        if (this.applicationId == null || this.applicationId.equals("")) {
            if (this.runtimeName == null || this.runtimeName.equals("")) {
                return this.serviceName;
            }
            return this.runtimeName + "_" + this.serviceName;
        }
        return this.applicationId + "_" + this.runtimeName + "_" + this.serviceName;
    }
}
