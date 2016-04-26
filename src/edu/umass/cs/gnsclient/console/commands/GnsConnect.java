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

import edu.umass.cs.gnsclient.client.GNSClientConfig;
import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
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
    return "Connects to the GNS; third argument should be true if the server"
            + " isn't using SSL (AKA SSL mode = CLEAR)";
  }

  @Override
  public String getCommandName() {
    return "gns_connect";
  }

  @Override
  public String getCommandParameters() {
    return "";
    //return "GnsHostName [GnsPortNumber] [disableSSL]";
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
      if (st.countTokens() > 0) {
        console.printString("THE USE OF host, port and disableSSL HAS BEEN DEPRECATED AND WILL"
                + " GO AWAY IN A FUTURE RELEASE! See instead gigapaxos.properties.\n");
      }

      GNSClientCommands gnsClient;
      gnsClient = new GNSClientCommands();
      if (!module.isSilent()) {
        console.printString("Checking GNS connectivity.\n");
      }
      gnsClient.checkConnectivity();
      if (!module.isSilent()) {
        console.printString("Connected to GNS.");
      }
      // Set the global variable for other activities
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + "Connected to GNS>");
      module.setGnsClient(gnsClient);

//      // If no default GNS has been defined yet, use this GNS as the default
//      if (KeyPairUtils.getDefaultGns() == null) {
//        module.setUseGnsDefaults(true);
//        KeyPairUtils.setDefaultGns(gnsHost + ":" + gnsPort + ":" + Boolean.toString(disableSSL));
//        module.printString(gnsHost + ":" + gnsPort + " saved as default GNS.\n");
//      }
    } catch (Exception e) {
      console.printString("Failed to connect to GNS ( " + e + ")\n");
      module.setGnsClient(null);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + "not connected to GNS>");
    }
  }
}
