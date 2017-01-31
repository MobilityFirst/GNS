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
 * Delete an account
 */
public class AccountDelete extends ConsoleCommand {

  /**
   * Creates a new <code>AccountDelete</code> object
   *
   * @param module
   */
  public AccountDelete(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Delete an account with a GUID.";
  }

  @Override
  public String getCommandName() {
    return "account_delete";
  }

  @Override
  public String getCommandParameters() {
    return "alias";
  }

  @Override
  public void parse(String commandText) throws Exception {
    StringTokenizer st = new StringTokenizer(commandText.trim());
    if ((st.countTokens() != 1)) {
      wrongArguments();
      return;
    }
    String aliasName = st.nextToken();
    try {
      GNSClientCommands gnsClient = module.getGnsClient();
      try {
        gnsClient.lookupGuid(aliasName);
      } catch (IOException | ClientException expected) {
        printString("Alias " + aliasName + " doesn't exist.\n");
        return;
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);
      if (myGuid == null) {
        printString("Unable to retrieve GUID for alias " + aliasName + "\n");
        return;
      }

      gnsClient.accountGuidRemove(myGuid);
      KeyPairUtils.removeKeyPair(module.getGnsInstance(), aliasName);
      module.setCurrentGuidAndCheckForVerified(null);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + ">");
      if (!module.isSilent()) {
        printString("Removed account GUID " + myGuid.getGuid() + "\n");
      }
    } catch (ClientException | IOException e) {
      printString("Failed to delete guid ( " + e + ")\n");
    }
  }
}
