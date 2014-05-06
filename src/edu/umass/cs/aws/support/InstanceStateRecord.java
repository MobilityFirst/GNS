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
