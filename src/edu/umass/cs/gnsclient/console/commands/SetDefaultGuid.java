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
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Command that sets the default GUID in the GNS user preferences
 */
public class SetDefaultGuid extends ConsoleCommand {

  /**
   * Creates a new <code>SetDefaultGuid</code> object
   *
   * @param module
   */
  public SetDefaultGuid(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Set the default alias/GUID to use in your preferences.";
  }

  @Override
  public String getCommandName() {
    return "set_default_guid";
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

    StringTokenizer st = new StringTokenizer(commandText.trim());
    if (st.countTokens() != 1) {
      wrongArguments();
      return;
    }
    String aliasName = st.nextToken();
    GNSClientCommands gnsClient = module.getGnsClient();

    try {
      try {
        gnsClient.lookupGuid(aliasName);
      } catch (ClientException e) {
        console.printString("Alias " + aliasName + " is not registered in the GNS\n");
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
      // module.setCurrentGuid(myGuid);
      module.setDefaultGuidAndCheckForVerified(myGuid);
      if (!module.isSilent()) {
        console.printString("Default GUID set to " + aliasName + "\n");
      }
    } catch (IOException e) {
      console.printString("Failed to access the GNS ( " + e + ")\n");
    }
  }
}
