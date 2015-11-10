/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.aws.support;

/**
 *
 * @author westy
 */
 public enum InstanceStateRecord {

    PENDING(0, "pending"),
    RUNNING(16, "running"),
    SHUTTINGDOWN(32, "shutting-down"),
    TERMINATED(48, "terminated"),
    STOPPING(64, "stopping"),
    STOPPED(80, "stopped");
    private Integer code;
    private String name;

    private InstanceStateRecord(Integer code, String name) {
      this.code = code;
      this.name = name;
    }

    public Integer getCode() {
      return code;
    }

    public String getName() {
      return name;
    }
  }
