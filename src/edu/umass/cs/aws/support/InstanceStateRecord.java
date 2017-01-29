
package edu.umass.cs.aws.support;


 public enum InstanceStateRecord {


  PENDING(0, "pending"),


  RUNNING(16, "running"),


  SHUTTINGDOWN(32, "shutting-down"),


  TERMINATED(48, "terminated"),


  STOPPING(64, "stopping"),


  STOPPED(80, "stopped");
    private final Integer code;
    private final String name;

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
