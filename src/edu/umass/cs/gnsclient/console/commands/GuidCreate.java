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

import java.security.PublicKey;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Command that creates a a new guid
 */
public class GuidCreate extends ConsoleCommand {

  /**
   * Creates a new <code>GuidCreate</code> object
   *
   * @param module
   */
  public GuidCreate(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Create a new GUID, associate an alias and register it in the GNS.";
  }

  @Override
  public String getCommandName() {
    return "guid_create";
  }

  @Override
  public String getCommandParameters() {
    return "alias";
  }

  /**
   * Override execute to check for existing connectivity
   *
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception {
    if (!module.isCurrentGuidSetAndVerified()) {
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    GuidEntry accountGuid = module.getCurrentGuid();
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
        printString("Alias " + aliasName + " already exists.\n");
        return;
      } catch (ClientException expected) {
        // The alias does not exists, that's good, let's create it
      }

      if (!module.isSilent()) {
        printString("Looking for alias " + aliasName + " GUID and certificates...\n");
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

      if (myGuid != null) {
        try {
          PublicKey pk = gnsClient.publicKeyLookupFromGuid(myGuid.getGuid());
          if (myGuid.getPublicKey().equals(pk)) { // We already have the key but the alias is missing in the GNS,
            // re-add the alias
            printString("Alias info found locally but missing in GNS, re-adding alias to the GNS\n");
            gnsClient.guidCreate(myGuid, aliasName);
          } else {
            printString("Old certificates found locally and not matching key in GNS, deleting local keys\n");
            KeyPairUtils.removeKeyPair(module.getGnsInstance(), aliasName);
          }
        } catch (InvalidGuidException e) {
          KeyPairUtils.removeKeyPair(module.getGnsInstance(), aliasName);
        }
      }

      if (!module.isSilent()) {
        printString("Generating new GUID and keys for account " + accountGuid.getEntityName() + " \n");
      }
      myGuid = gnsClient.guidCreate(accountGuid, aliasName);

      if (!module.isSilent()) {
        printString("Created GUID " + myGuid.getGuid() + "\n");
      }
      if (module.getCurrentGuid() == null) {
        module.setCurrentGuidAndCheckForVerified(myGuid);
        module.setPromptString(ConsoleModule.CONSOLE_PROMPT + aliasName + ">");
      }
    } catch (IOException | ClientException e) {
      printString("Failed to create new guid ( " + e + ")\n");
    }
  }
}
