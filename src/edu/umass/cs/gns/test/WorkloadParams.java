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

  public static final String OBJECT_SIZE = "object_size_kb";
  public static final String TTL = "ttl";

  private int objectSizeKB;

  private int ttl;

  public WorkloadParams(String workloadConfFile) throws IOException {
    if (workloadConfFile == null) return;

    File f = new File(workloadConfFile);
    if (!f.isFile()) return;

    Properties prop = new Properties();
    prop.load(new FileReader(workloadConfFile));

    if (prop.containsKey(OBJECT_SIZE)) {
      this.objectSizeKB = Integer.parseInt(prop.getProperty(OBJECT_SIZE));
      GNS.getLogger().info("Object size = " + objectSizeKB);
    } else this.objectSizeKB = 0;

    if (prop.containsKey(TTL)) {
      this.ttl = Integer.parseInt(prop.getProperty(TTL));
      GNS.getLogger().info("TTL value = " + ttl);
    } else this.ttl = GNS.DEFAULT_TTL_SECONDS;
  }

  public int getObjectSizeKB() {
    return objectSizeKB;
  }


  public int getTtl() {
    return ttl;
  }
}
