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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.util.Logging;
import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

/**
 * Contains logging and other main utilities for the GNS client.
 * 
 * @author westy
 */
public class GNSClient {
  
  public static final int LNS_PORT = 24398;
  public static final int ACTIVE_REPLICA_PORT = 24403;
  
  /**
   * Logging level for main logger
   */
  private static String fileLoggingLevel = "INFO";
  
  /**
   * Console output level for main logger
   */
  private static String consoleOutputLevel = "INFO";
  
  private final static Logger LOGGER = Logger.getLogger(GNSClient.class.getName());
  private static boolean initRun = false;
  
  public static Logger getLogger() {
    if (!initRun) {
      Logging.setupLogger(LOGGER, consoleOutputLevel, fileLoggingLevel, "log" + "/gns.xml");
      initRun = true;
    }
    return LOGGER;
  }
  
  public static String readBuildVersion() {
    String result = null;
    try {
      Class clazz = GNSClient.class;
      String className = clazz.getSimpleName() + ".class";
      String classPath = clazz.getResource(className).toString();
      //System.out.println("readBuildVersion: classPath is " + classPath);
      if (classPath.startsWith("jar")) {
        String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1)
                + "/META-INF/MANIFEST.MF";
        //System.out.println("readBuildVersion: manifestPath is " + manifestPath);
        Manifest manifest = new Manifest(new URL(manifestPath).openStream());
        Attributes attr = manifest.getMainAttributes();
        result = attr.getValue("Build-Version");
      }
    } catch (IOException e) {
    }
    return result;
  }
}
