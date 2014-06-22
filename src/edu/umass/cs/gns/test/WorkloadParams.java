package edu.umass.cs.gns.test;

import edu.umass.cs.gns.main.GNS;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Lists workload parameters for running tests with the system.
 *
 * Created by abhigyan on 4/13/14.
 */
public class WorkloadParams {

  public static final String EXP_TYPE = "exp_type";

  // parameters for trace-based experiment
  public static final String OBJECT_SIZE = "object_size_kb";
  public static final String TTL = "ttl";

  // parameters for trace-based experiment
  public static final String MOBILE_ID = "mobile_id";
  public static final String CORRESPONDENT_ID = "correspondent_id";
  public static final String MOBILE_UPDATE_INTERVAL = "mobile_update_interval";

  private ExpType expType = null;

  // parameters for trace-based experiment
  private int objectSizeKB;
  private int ttl;

  // parameters for connect-time experiment
  private int mobileId = 3;
  private int correspondentId = 4;
  private double mobileUpdateInterval = 4;

  public WorkloadParams(String workloadConfFile) throws IOException {
    if (workloadConfFile == null) return;

    File f = new File(workloadConfFile);
    if (!f.isFile()) return;

    Properties prop = new Properties();
    prop.load(new FileReader(workloadConfFile));

    if (prop.containsKey(EXP_TYPE)) {
      this.expType = ExpType.getExpType(prop.getProperty(EXP_TYPE));
    }
    readConnectTimeExpParameters(prop);
    readTraceExpParameters(prop);
  }

  /**
   * Reads parameters related to trace experiment
   */
  private void readTraceExpParameters(Properties prop) {

    if (prop.containsKey(OBJECT_SIZE)) {
      this.objectSizeKB = Integer.parseInt(prop.getProperty(OBJECT_SIZE));
      GNS.getLogger().info("Object size = " + objectSizeKB);
    } else this.objectSizeKB = 0;

    if (prop.containsKey(TTL)) {
      this.ttl = Integer.parseInt(prop.getProperty(TTL));
      GNS.getLogger().info("TTL value = " + ttl);
    } else this.ttl = GNS.DEFAULT_TTL_SECONDS;

  }

  /**
   * Reads parameters related to connect time experiment
   */
  private void readConnectTimeExpParameters(Properties prop) {
    if (prop.containsKey(MOBILE_UPDATE_INTERVAL)) {
      this.mobileUpdateInterval = Double.parseDouble(prop.getProperty(MOBILE_UPDATE_INTERVAL));
    }
    if (prop.containsKey(MOBILE_ID)) {
      this.mobileId = Integer.parseInt(prop.getProperty(MOBILE_ID));
    }
    if (prop.containsKey(CORRESPONDENT_ID)) {
      this.correspondentId = Integer.parseInt(prop.getProperty(CORRESPONDENT_ID));
    }

  }

  public int getObjectSizeKB() {
    return objectSizeKB;
  }


  public int getTtl() {
    return ttl;
  }


  public ExpType getExpType() {
    return expType;
  }

  public int getMobileId() {
    return mobileId;
  }

  public int getCorrespondentId() {
    return correspondentId;
  }

  public double getMobileUpdateInterval() {
    return mobileUpdateInterval;
  }

}
