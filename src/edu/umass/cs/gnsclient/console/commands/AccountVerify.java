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

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.VerificationException;

/**
 * Updates the account verification status
 */
public class AccountVerify extends ConsoleCommand {

  /**
   * Creates a new <code>AccountVerify</code> object
   *
   * @param module
   */
  public AccountVerify(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Verify the creation of an account";
  }

  @Override
  public String getCommandName() {
    return "account_verify";
  }

  @Override
  public String getCommandParameters() {
    return "alias code";
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
      GNSClientCommands client = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 2) {
        wrongArguments();
        return;
      }
      String alias = st.nextToken();
      GuidEntry guid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), alias);
      if (guid == null) {
        printString("Unable to retrieve GUID for alias " + alias + "\n");
        return;
      }

      String code = st.nextToken();

      try {
        if (client.accountGuidVerify(guid, code).startsWith(GNSProtocol.OK_RESPONSE.toString())) {
          printString("Account verified.\n");
          module.setAccountVerified(true);
          return;
        }
        // this happens if it was already verified, but we didn't notice
      } catch (VerificationException e) {
        module.setAccountVerified(true);
        printString("Account already verified"+"\n");
        return;
      } catch (Exception e) {
        printString("Account not verified: " + e + "\n");
      }
    } catch (Exception e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }

    module.setAccountVerified(false);
  }
}
