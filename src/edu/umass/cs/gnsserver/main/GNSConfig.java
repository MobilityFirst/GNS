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
package edu.umass.cs.gnsserver.main;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

/**
 * Contains config parameters for the nameservers and logging functionality.
 */
public class GNSConfig {

  /**
   * The default TTL.
   */
  public static final int DEFAULT_TTL_SECONDS = 0;

  /**
   * The default starting port.
   */
  public static final int DEFAULT_STARTING_PORT = 24400;
  /**
   * The URL path used by the HTTP server.
   */
  public static final String GNS_URL_PATH = "GNS";
  // Useful for testing with resources in conf/testCodeResources if using 
  // "import from build file in IDE". Better way to do this?
  /**
   * Hack.
   */
  public static final String ARUN_GNS_DIR_PATH = "/Users/arun/GNS/";
  /**
   * Hack.
   */
  public static final String WESTY_GNS_DIR_PATH = "/Users/westy/Documents/Code/GNS";
  /**
   * The maximum number of HRN aliases allowed for a guid.
   */
  public static int MAXALIASES = 100;
  /**
   * The maximum number of subguids allowed in an account guid.
   * The upper limit on this is currently dictated by mongo's 16MB document limit.
   * https://docs.mongodb.org/manual/reference/limits/#bson-documents
   */
  public static int MAXGUIDS = 300000;

  // This is designed so we can run multiple NSs on the same host if needed
  /**
   * Master port types.
   */
  public enum PortType {
    /**
     * Port used to send requests to an active replica.
     */
    ACTIVE_REPLICA_PORT(0),
    /**
     * Port used to requests to a reconfigurator replica.
     */
    RECONFIGURATOR_PORT(1),
    // Reordered these so they work with the new GNSApp
    /**
     * Port used to send requests to a name server.
     */
    NS_TCP_PORT(3), // TCP port at name servers
    /**
     * Port used to send admin requests to a name server.
     */
    NS_ADMIN_PORT(4),
    // sub ports
    /**
     * Port used to send requests to a command pre processor.
     */
    CCP_PORT(6),
    /**
     * Port used to send admin requests to a command pre processor.
     */
    CCP_ADMIN_PORT(7);

    //
    int offset;

    PortType(int offset) {
      this.offset = offset;
    }

    /**
     * Returns the max port offset.
     *
     * @return an int
     */
    public static int maxOffset() {
      int result = 0;
      for (PortType p : values()) {
        if (p.offset > result) {
          result = p.offset;
        }
      }
      return result;
    }

    /**
     * Returns the offset for this port.
     *
     * @return an int
     */
    public int getOffset() {
      return offset;
    }
  }
  /**
   * Controls whether email verification is enabled.
   */
  public static boolean enableEmailAccountVerification = true;
  /**
   * Controls whether signature verification is enabled.
   */
  public static boolean enableSignatureAuthentication = true;
  /**
   * Number of primary nameservers. Default is 3 *
   */
  public static final int DEFAULT_NUM_PRIMARY_REPLICAS = 3;

  /**
   * The current number of primary replicas that should be created for a name.
   */
  public static int numPrimaryReplicas = DEFAULT_NUM_PRIMARY_REPLICAS;
  /**
   * Default query timeout in ms. How long we wait before retransmitting a query.
   */
  public static int DEFAULT_QUERY_TIMEOUT = 2000;
  /**
   * Maximum query wait time in milliseconds. After this amount of time
   * a negative response will be sent back to a client indicating that a
   * record could not be found.
   */
  public static int DEFAULT_MAX_QUERY_WAIT_TIME = 16000; // was 10

  private final static Logger LOG = Logger.getLogger(GNSConfig.class.getName());

  /**
   * Returns the master GNS logger.
   *
   * @return the master GNS logger
   */
  public static Logger getLogger() {
    return LOG;
  }

  /**
   * Attempts to look for a MANIFEST file in that contains the Build-Version attribute.
   *
   * @return a build version
   */
  public static String readBuildVersion() {
    String result = null;
    Enumeration<URL> resources = null;
    try {
      resources = GNSConfig.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
    } catch (IOException E) {
      // handle
    }
    if (resources != null) {
      while (resources.hasMoreElements()) {
        try {
          Manifest manifest = new Manifest(resources.nextElement().openStream());
          // check that this is your manifest and do what you need or get the next one
          Attributes attr = manifest.getMainAttributes();
          result = attr.getValue("Build-Version");
        } catch (IOException E) {
          // handle
        }
      }
    }
    return result;
  }

//    String result = null;
//    try {
//      Class<?> clazz = GNSConfig.class;
//      String className = clazz.getSimpleName() + ".class";
//      String classPath = clazz.getResource(className).toString();
//      if (classPath.startsWith("jar")) {
//        String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1)
//                + "/META-INF/MANIFEST.MF";
//        Manifest manifest = new Manifest(new URL(manifestPath).openStream());
//        Attributes attr = manifest.getMainAttributes();
//        result = attr.getValue("Build-Version");
//      }
//    } catch (IOException e) {
//    }
//    return result;
// }
}
