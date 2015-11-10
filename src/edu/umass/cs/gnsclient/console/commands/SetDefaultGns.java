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
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Command that sets the default GNS to use in the user preferences
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class SetDefaultGns extends ConsoleCommand {

  /**
   * Creates a new <code>GnsConnect</code> object
   *
   * @param module
   */
  public SetDefaultGns(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Sets the default GNS to use in the user preferences.";
  }

  @Override
  public String getCommandName() {
    return "set_default_gns";
  }

  @Override
  public String getCommandParameters() {
    return "GnsHostName GnsPortNumber [disableSSL]";
  }

  /**
   * Override execute to not check for existing connectivity
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      StringTokenizer st = new StringTokenizer(commandText);
      if (st.countTokens() != 2 && st.countTokens() != 3) {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String gnsHost = st.nextToken();
      int gnsPort = Integer.valueOf(st.nextToken());
      boolean disableSSL = false;
      if (st.countTokens() == 1) {
        disableSSL = Boolean.valueOf(st.nextToken());
      }
      // Create a client
      UniversalTcpClient gnsClient = new UniversalTcpClient(gnsHost, gnsPort, disableSSL);
      if (!module.isSilent()) {
        console.printString("Checking GNS connectivity.\n");
      }
      gnsClient.checkConnectivity();
      if (!module.isSilent()) {
        console.printString("Connected sucesssfully to GNS at " + gnsHost + ":" + gnsPort + "\n");
      }

      module.setUseGnsDefaults(true);
      KeyPairUtils.setDefaultGns(gnsHost + ":" + gnsPort + ":" + Boolean.toString(disableSSL));
      if (!module.isSilent()) {
        console.printString("Default GNS set to " + gnsHost + ":" + gnsPort + "\n");
      }
    } catch (Exception e) {
      console.printString("Failed to connect to GNS ( " + e + ")\n");
    }
  }
}
