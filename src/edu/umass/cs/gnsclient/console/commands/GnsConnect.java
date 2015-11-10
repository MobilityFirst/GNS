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

import edu.umass.cs.gnsclient.client.GNSClient;
import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Command that connects to the server
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class GnsConnect extends ConsoleCommand {

  /**
   * Creates a new <code>GnsConnect</code> object
   *
   * @param module
   */
  public GnsConnect(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Connects to the GNS; third argument should be true if you're connecting to a server that"
            + " isn't using SSL (AKA SSL mode = CLEAR)";
  }

  @Override
  public String getCommandName() {
    return "gns_connect";
  }

  @Override
  public String getCommandParameters() {
    return "GnsHostName [GnsPortNumber] [disableSSL]";
  }

  /**
   * Override execute to not check for existing connectivity
   *
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
      if (st.countTokens() != 1 && st.countTokens() != 2 && st.countTokens() != 3) {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String gnsHost = st.nextToken();
      int gnsPort;
      if (st.countTokens() == 1 || st.countTokens() == 2) {
        gnsPort = Integer.valueOf(st.nextToken());
      } else {
        gnsPort = GNSClient.LNS_PORT;
      }
      boolean disableSSL = false;
      if (st.countTokens() == 1) {
        disableSSL = Boolean.valueOf(st.nextToken());
      }
      UniversalTcpClient gnsClient;
      gnsClient = new UniversalTcpClient(gnsHost, gnsPort, disableSSL);
      if (!module.isSilent()) {
        console.printString("Checking GNS connectivity.\n");
      }
      gnsClient.checkConnectivity();
      if (!module.isSilent()) {
        console.printString("Connected to GNS at " + gnsHost + ":" + gnsPort +"\n");
      }
      // Set the global variable for other activities
      //UniversalTcpClient.setGns(gnsClient);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + "Connected to " + gnsHost + ":" + gnsPort + ">");
      module.setGnsClient(gnsClient);

      // If no default GNS has been defined yet, use this GNS as the default
      if (KeyPairUtils.getDefaultGns() == null) {
        module.setUseGnsDefaults(true);
        KeyPairUtils.setDefaultGns(gnsHost + ":" + gnsPort + ":" + Boolean.toString(disableSSL));
        module.printString(gnsHost + ":" + gnsPort + " saved as default GNS.\n");
      }
    } catch (Exception e) {
      console.printString("Failed to connect to GNS ( " + e + ")\n");
      module.setGnsClient(null);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + "not connected to GNS>");
    }
  }
}
