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

import edu.umass.cs.gnsclient.client.GNSClient;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import javax.swing.JOptionPane;
import edu.umass.cs.gnscommon.utils.NetworkUtils;

/**
 *
 * @author westy
 */
public class ServerSelectDialog {

  private static final String[] hostStringOptions = {
    "gnserve.net:" + GNSClient.LNS_PORT + " (LNS)",
    "kittens.name:" + GNSClient.LNS_PORT + " (LNS)",
    "gdns.name:" + GNSClient.LNS_PORT + " (LNS)",
    "hazard.hpcc.umass.edu:" + GNSClient.LNS_PORT + " (LNS)",
    getLocalHostAddressString() + ":" + GNSClient.LNS_PORT + " (LNS)",
    "gnserve.net:" + GNSClient.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "kittens.name:" + GNSClient.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "gdns.name:" + GNSClient.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "hazard.hpcc.umass.edu:" + GNSClient.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    getLocalHostAddressString() + ":" + GNSClient.ACTIVE_REPLICA_PORT + " (AR - single node test only)",
    "Other"
  };

  public static InetSocketAddress selectServer() {
    try {
      String hostPort = ChooserDialog.showDialog(null,
              null,
              "Choose a GNS Server Host",
              "GNS Server Selection",
              hostStringOptions,
              hostStringOptions[1],
              null);
      if (hostPort == "Other") {
        hostPort = JOptionPane.showInputDialog(null, "Enter host:port (" + GNSClient.LNS_PORT + " is the default for TCP clients)");
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
      return new InetSocketAddress(getLocalHostAddressString(), GNSClient.LNS_PORT);
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

//  
//  private static JTextField host = new JTextField();
//  private static Object[] hostOptions = {
//    getLocalHostAddressString() + ":8080", "gnserve.net:8080", "kittens.name:8080",
//    getLocalHostAddressString() + ":35031", "gnserve.net:24401", "kittens.name:24401", "Other"
//  //, host - fix this later
//  };
//
//  final static JOptionPane optionPane = new JOptionPane(
//          "Choose a GNS Server Host",
//          JOptionPane.QUESTION_MESSAGE,
//          JOptionPane.DEFAULT_OPTION,
//          null,
//          hostOptions,
//          hostOptions[1]);
//
//  static Dialog nullDialog = null; // a little hack for the constructor
//
//  public static InetSocketAddress selectServer() {
//    final JDialog dialog = new JDialog(nullDialog, true);
//    dialog.setContentPane(optionPane);
//    dialog.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
//    dialog.setLocationRelativeTo(null);
//    optionPane.addPropertyChangeListener(
//            new PropertyChangeListener() {
//              @Override
//              public void propertyChange(PropertyChangeEvent e) {
//                String prop = e.getPropertyName();
//
//                if (dialog.isVisible()
//                && (e.getSource() == optionPane)
//                && (prop.equals(JOptionPane.VALUE_PROPERTY))) {
//                    //If you were going to check something
//                  //before closing the window, you'd do
//                  //it here.
//                  dialog.setVisible(false);
//                }
//              }
//            });
//    dialog.pack();
//    dialog.setVisible(true);
//
//    String hostPort = (String) optionPane.getValue();
//    if (hostPort == "Other") {
//      hostPort = JOptionPane.showInputDialog(nullDialog, "Enter host:port");
//    }
//    String[] hostPortTuple = hostPort.split(":");
//    if (hostPortTuple.length > 1) {
//      return new InetSocketAddress(hostPortTuple[0], Integer.parseInt(hostPortTuple[1]));
//    } else {
//      throw new RuntimeException("Bad host:port specification: " + hostPort);
//    }
//  }
//
//  // also can do this, but doesn't allow for customizing or adding elements to the window
//  public static InetSocketAddress selectServerSimple() {
//    int n = JOptionPane.showOptionDialog(null,
//            "Choose a GNS Server Host",
//            "GNS Server Selection",
//            JOptionPane.DEFAULT_OPTION,
//            JOptionPane.QUESTION_MESSAGE,
//            null,
//            hostOptions,
//            hostOptions[1]);
//    String hostPort = (String) hostOptions[n];
//    String[] hostPortTuple = hostPort.split(":");
//    return new InetSocketAddress(hostPortTuple[0], Integer.parseInt(hostPortTuple[0]));
//  }
}
