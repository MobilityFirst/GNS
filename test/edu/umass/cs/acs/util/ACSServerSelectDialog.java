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
package edu.umass.cs.acs.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.swing.JOptionPane;


/**
 *
 * @author westy
 */
public class ACSServerSelectDialog {

  private static final String[] hostStringOptions = {
    "hazard.hpcc.umass.edu",
    "server.casa.umass.edu",
    "127.0.0.1",
    "Other"
  };

  public static InetAddress selectServer() {
    String host = ChooserDialog.showDialog(null,
            null,
            "Choose a ACS Server Host",
            "ACS Server Selection",
            hostStringOptions,
            hostStringOptions[1],
            null);
    if (host == "Other") {
      host = JOptionPane.showInputDialog(null, "Enter host");
    }
    if (host == null) {
      throw new RuntimeException("No host choosen!");
    }
    InetAddress address = null;
    try {
      address = InetAddress.getByName(host);
    } catch (UnknownHostException e) {
    }
    if (address == null) {
      throw new RuntimeException("Invalid host " + host);
    }
    return address;
  }
  
  // test code
  public static void main(String[] args) throws Exception {
    System.out.println(selectServer());
    System.exit(0);
  }

}
