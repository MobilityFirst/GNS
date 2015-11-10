/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
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
