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
package edu.umass.cs.gnsclient.client.util;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import javax.swing.JOptionPane;
import edu.umass.cs.gnscommon.utils.NetworkUtils;

/**
 *
 * @author westy
 */
public class ServerSelectDialog {

	/* arun: It can not be correct to use hard-coded port values here. 
	 * Deprecate this class or read the correct values from the 
	 * properties file.
	 */
  private static final String[] HOST_STRING_OPTIONS = {
    "gnserve.net:" + GNSClientConfig.LNS_PORT + " (LNS)",
    "kittens.name:" + GNSClientConfig.LNS_PORT + " (LNS)",
    "gdns.name:" + GNSClientConfig.LNS_PORT + " (LNS)",
    "hazard.hpcc.umass.edu:" + GNSClientConfig.LNS_PORT + " (LNS)",
    getLocalHostAddressString() + ":" + GNSClientConfig.LNS_PORT + " (LNS)",
    "gnserve.net:" + GNSClientConfig.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "kittens.name:" + GNSClientConfig.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "gdns.name:" + GNSClientConfig.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "hazard.hpcc.umass.edu:" + GNSClientConfig.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    getLocalHostAddressString() + ":" + GNSClientConfig.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "Other"
  };

  public static InetSocketAddress selectServer() {
    try {
      String hostPort = ChooserDialog.showDialog(null,
              null,
              "Choose a GNS Server Host",
              "GNS Server Selection",
              HOST_STRING_OPTIONS,
              HOST_STRING_OPTIONS[1],
              null);
      if (hostPort.equals("Other")) {
        hostPort = JOptionPane.showInputDialog(null, "Enter host:port (" + GNSClientConfig.LNS_PORT + " is the default for TCP clients)");
      }
      if (hostPort == null) {
        throw new RuntimeException("No host:port choosen!");
      }
      InetSocketAddress address = parseHostPortTuple(hostPort);
      if (address == null) {
        throw new RuntimeException("Invalid host:port: " + hostPort);
      }
      return address;
    } catch (RuntimeException e) {
      return new InetSocketAddress(getLocalHostAddressString(), GNSClientConfig.LNS_PORT);
    }
  }

  /**
   * Parses a host:port string. Returns null if a valid host port is not supplied.
   *
   * @param hostPortTuple
   * @return
   */
  public static InetSocketAddress parseHostPortTuple(String hostPortTuple) {
    String[] hostPortArray = hostPortTuple.split("[:\\s]"); // split on : and white spaces
    if (hostPortArray.length > 1) {
      // first element should be the host, second should be the port, remaining elements we ignore
      return new InetSocketAddress(hostPortArray[0], Integer.parseInt(hostPortArray[1]));
    } else {
      return null;
    }
  }

  private static String getLocalHostAddressString() {
    try {
      return NetworkUtils.getLocalHostLANAddress().getHostAddress();
    } catch (UnknownHostException e) {
      return "127.0.0.1";
    }
  }

  // test code
  public static void main(String[] args) throws Exception {
    System.out.println(selectServer());
    System.exit(0);
  }
}
