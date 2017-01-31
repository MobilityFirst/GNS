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

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Command that saves GUID/alias/Keypair information to a file
 */
public class PrivateKeyExport extends ConsoleCommand {

  /**
   * Creates a new <code>PrivateKeyExport</code> object
   *
   * @param module
   */
  public PrivateKeyExport(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Saves private key for guid into a file on disk";
  }

  @Override
  public String getCommandName() {
    return "private_key_export";
  }

  @Override
  public String getCommandParameters() {
    return "alias path_and_filename";
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
      if (st.countTokens() != 2) {
        wrongArguments();
        return;
      }
      String aliasName = st.nextToken();
      String filename = st.nextToken();

      if (!module.isSilent()) {
        console.printString("Looking up alias " + aliasName + " GUID and certificates...\n");
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

      if (myGuid == null) {
        console.printString("You do not have the private key for alias " + aliasName);
        console.printNewline();
        return;
      }

      if (KeyPairUtils.writePrivateKeyToPKCS8File(myGuid.getPrivateKey(), filename)) {
        console.printString("Private key for " + aliasName + " stored in " + filename + " in "
                + myGuid.getPrivateKey().getFormat() + " format.");
        console.printNewline();
      }
    } catch (Exception e) {
      console.printString("Failed to save keys ( " + e + ")\n");
    }
  }
}
