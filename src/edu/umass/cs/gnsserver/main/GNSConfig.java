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
package edu.umass.cs.gnsserver.main;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.utils.Config;

/**
 * @author arun, westy
 * 
 *         Contains config parameters for the nameservers and logging
 *         functionality. This file should be used for server-only properties.
 *         Client properties are {@link GNSClientConfig}.
 */
public class GNSConfig {

	/**
	 *
	 */
	public static enum GNSC implements Config.ConfigurableEnum {

		/**
		 * Enables secret key communication that is ~180x faster at signing and
		 * ~8x faster at verification. True by default as there is no reason to
		 * not support it at the server.
		 */
		ENABLE_SECRET_KEY(true),

		/**
		 * Uses DiskMapRecords if enabled.
		 */
		ENABLE_DISKMAP(false),
		/**
		 * Completely turns off mongo or other persistent database provided
		 * DiskMap is also enabled.
		 */
		IN_MEMORY_DB(false),

		/**
		 * If enabled, the GNS will cache and return the same value for reads.
		 * 
		 * Code-breaking if enabled. Meant only for instrumentation.
		 */
		EXECUTE_NOOP_ENABLED(false),

		/**
		 * A secret shared between the server and client in order to circumvent
		 * account verification. Must be changed using properties file if manual
		 * verification is disabled.
		 */
		VERIFICATION_SECRET(
				"AN4pNmLGcGQGKwtaxFFOKG05yLlX0sXRye9a3awdQd2aNZ5P1ZBdpdy98Za3qcE"
						+ "o0u6BXRBZBrcH8r2NSbqpOoWfvcxeSC7wSiOiVHN7fW0eFotdFz0fiKjHj3h0ri")

		;

		final Object defaultValue;

		GNSC(Object defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}

		// FIXME: a better default name?
		@Override
		public String getConfigFileKey() {
			return "gigapaxosConfig";
		}

		@Override
		public String getDefaultConfigFile() {
			return "gns.server.properties";
		}
	}

	/* FIXME: arun: some parameters below are not relevant any more and need to
	 * go. I removed the ones not being using static analysis. */

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
	public static final String WESTY_GNS_DIR_PATH = "/Users/westy/Documents/Code/GNS";
	/**
	 * The maximum number of HRN aliases allowed for a guid.
	 */
	public static int MAXALIASES = 100;
	/**
	 * The maximum number of subguids allowed in an account guid. The upper
	 * limit on this is currently dictated by mongo's 16MB document limit.
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
	 * Default query timeout in ms. How long we wait before retransmitting a
	 * query.
	 */
	public static int DEFAULT_QUERY_TIMEOUT = 2000;
	/**
	 * Maximum query wait time in milliseconds. After this amount of time a
	 * negative response will be sent back to a client indicating that a record
	 * could not be found.
	 */
	public static int DEFAULT_MAX_QUERY_WAIT_TIME = 16000; // was 10

	private final static Logger LOG = Logger.getLogger(GNSConfig.class
			.getName());

	/**
	 * Returns the master GNS logger.
	 *
	 * @return the master GNS logger
	 */
	public static Logger getLogger() {
		return LOG;
	}

	/**
	 * Attempts to look for a MANIFEST file in that contains the Build-Version
	 * attribute.
	 *
	 * @return a build version
	 */
	public static String readBuildVersion() {
		String result = null;
		Enumeration<URL> resources = null;
		try {
			resources = GNSConfig.class.getClassLoader().getResources(
					"META-INF/MANIFEST.MF");
		} catch (IOException E) {
			// handle
		}
		if (resources != null) {
			while (resources.hasMoreElements()) {
				try {
					Manifest manifest = new Manifest(resources.nextElement()
							.openStream());
					// check that this is your manifest and do what you need or
					// get the next one
					Attributes attr = manifest.getMainAttributes();
					result = attr.getValue("Build-Version");
				} catch (IOException E) {
					// handle
				}
			}
		}
		return result;
	}
}
