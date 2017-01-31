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

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import java.util.StringTokenizer;

/**
 * Command that sets the current GUID to use for GNS commands.
 */
public class GuidUse extends ConsoleCommand {

  /**
   * Creates a new <code>GuidUse</code> object
   *
   * @param module
   */
  public GuidUse(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Use a different alias/GUID to execute GNS commands.";
  }

  @Override
  public String getCommandName() {
    return "guid_use";
  }

  @Override
  public String getCommandParameters() {
    return "alias";
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
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if ((st.countTokens() != 1)) {
        wrongArguments();
        return;
      }
      String aliasName = st.nextToken();
      GNSClientCommands gnsClient = module.getGnsClient();

      try {
        gnsClient.lookupGuid(aliasName);
      } catch (ClientException expected) {
        console.printString("Alias " + aliasName + " is not registered in the GNS");
        console.printNewline();
        return;
      }

      if (!module.isSilent()) {
        console.printString("Looking up alias " + aliasName + " GUID and certificates...\n");
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

      if (myGuid == null) {
        console.printString("You do not have the private key for alias " + aliasName);
        console.printNewline();
        return;
      }

      // Success, let's update the console prompt with the new alias name
      module.setCurrentGuidAndCheckForVerified(myGuid);
      console.printString("Current GUID set to " + myGuid);
      console.printNewline();
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + aliasName + ">");
    } catch (Exception e) {
      console.printString("Failed to access the GNS ( " + e + ")\n");
    }
  }
}
