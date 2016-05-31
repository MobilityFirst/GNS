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

import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Config;

/**
 * Contains logging and other main utilities for the GNS client. This file
 * should be used only for client properties. Server properties are in
 * {@link GNSConfig}.
 * 
 * @author westy, arun
 */
public class GNSClientConfig {
  
  public static final int LNS_PORT = 24398;
	// arun: It is almost never okay to use a hard-coded active replica port.
  public static final int ACTIVE_REPLICA_PORT = 24403;
  
  private final static Logger LOG = Logger.getLogger(GNSClientConfig.class.getName());
  
  /**
   * @return Logger used by most of the client package.
   */
  public static final Logger getLogger() {
    return LOG;
  }
  
	/**
	 *
	 */
	public static enum GNSCC implements Config.ConfigurableEnum {
		/**
		 * Used by AbstractGNSClient to use a single global monitor (bad old style) for all requests.
		 */
		USE_GLOBAL_MONITOR(false),
		
		/**
		 * Uses secret keys for signatures that are ~180x faster at signing at
		 * the client.
		 */
		ENABLE_SECRET_KEY (false),
		;

		final Object defaultValue;

		GNSCC(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}

		@Override
		public String getConfigFileKey() {
			return "gigapaxosConfig";
		}

		@Override
		public String getDefaultConfigFile() {
			return "gns.client.properties";
		}
	}

  
  /**
   * @return Try to figure out the build version.
   */
  public static String readBuildVersion() {
    String result = null;
    try {
      Class<?> clazz = GNSClientConfig.class;
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
