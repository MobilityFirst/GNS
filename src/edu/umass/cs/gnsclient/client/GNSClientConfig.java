/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnsclient.client;

import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;
import java.util.Enumeration;

/**
 * Contains logging and other main utilities for the GNS client. This file
 * should be used only for client properties. Server properties are in
 * {@link GNSConfig}.
 *
 * @author westy, arun
 */
public class GNSClientConfig {
  
  private final static Logger LOGGER = Logger.getLogger(GNSClientConfig.class
          .getName());

  /**
   * @return Logger used by most of the client package.
   */
  public static final Logger getLogger() {
    return LOGGER;
  }

  /**
   * The GNS Client Config.
   */
  public static enum GNSCC implements Config.ConfigurableEnum {
    /**
     * Used by AbstractGNSClient to use a single global monitor (bad old
     * style) for all requests.
     */
    USE_GLOBAL_MONITOR(false),
    /**
     * Uses secret keys for signatures that are ~180x faster at signing at
     * the client.
     */
    ENABLE_SECRET_KEY(true),
    /**
     * A secret shared between the server and client in order to circumvent
     * account verification. Must be changed using properties file if
     * manual verification is disabled.
     *
     * Security requirements:
     *
     * (1) The security of the account verification depends on the secrecy of
     * this secret, so the default value must be changed via the properties
     * file in a production setting.
     *
     * (2) SERVER_AUTH SSL must be enabled between clients and servers.
     */
    //@Deprecated
    //VERIFICATION_SECRET("EXPOSED_SECRET"),
    /**
     * Byteification mode for "important" packets like CommandPacket,
     * CommandValueReturnPacket, etc.
     */
    BYTE_MODE(0),
    /**
     * If set to true, the client uses java preferences to store keys rather than DerbyDB.
     * Specifically, KeyPairUtils class uses JavaPreferences instead of DerbyDB.
     *
     */
    USE_JAVA_PREFERENCE(false),
    /**
     * The port used by the local name server.
     */
    LOCAL_NAME_SERVER_PORT(24398),

    ENABLE_CROSS_ORIGIN_REQUESTS(false);
    
    final Object defaultValue;
    
    GNSCC(Object defaultValue) {
      this.defaultValue = defaultValue;
    }

    /**
     *
     * @return the default value
     */
    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }

    /**
     *
     * @return the config key file
     */
    @Override
    public String getConfigFileKey() {
      return "gigapaxosConfig";
    }

    /**
     *
     * @return the default config file
     */
    @Override
    public String getDefaultConfigFile() {
      return "gns.client.properties";
    }
  }

  /**
   * Try to figure out the build version.
   *
   * @return the build version
   */
  public static String readBuildVersion() {
    String result = "Unknown";
    try {
      Class<?> clazz = GNSClientConfig.class;
      //String className = clazz.getSimpleName() + ".class";
      //String classPath = clazz.getResource(className).toString();
      //System.out.println("readBuildVersion: classPath is " + classPath);
      // Look in all the resources that have META-INF to try to find the
      // gnsclient jar file.
      Enumeration<URL> urls = clazz.getClassLoader().getResources("META-INF");
      while (urls.hasMoreElements()) {
        URL url = urls.nextElement();
        //System.out.println("url: " + url.toString());
        String classPath = url.toString();
        if (classPath.startsWith("jar") && classPath.contains("gnsclient-")) {
          String manifestPath = classPath.substring(0,
                  classPath.lastIndexOf("!") + 1)
                  + "/META-INF/MANIFEST.MF";
          //System.out.println("readBuildVersion: manifestPath is " + manifestPath);
          Manifest manifest = new Manifest(new URL(manifestPath).openStream());
          Attributes attr = manifest.getMainAttributes();
          return attr.getValue("Build-Version");
        }
      }
    } catch (IOException e) {
    }
    return result;
  }
}
