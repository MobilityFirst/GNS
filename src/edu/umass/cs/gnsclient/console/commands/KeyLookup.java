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
import java.util.StringTokenizer;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnsclient.console.GnsUtils;

/**
 * Lookup a Public Key for an alias or GUID
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class KeyLookup extends ConsoleCommand {

  /**
   * Creates a new <code>KeyLookup</code> object
   *
   * @param module
   */
  public KeyLookup(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Lookup the public key of an alias or GUID (defaults to the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "key_lookup";
  }

  @Override
  public String getCommandParameters() {
    return "[alias_or_guid]";
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String alias;
      if (st.countTokens() == 0) {
        alias = module.getCurrentGuid().getEntityName();
      } else if (st.countTokens() != 1) {
        alias = st.nextToken();
      } else {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      UniversalTcpClient gnsClient = module.getGnsClient();
      PublicKey pk;
      if (!GnsUtils.isValidGuidString(alias)) {
        pk = gnsClient.publicKeyLookupFromAlias(alias);
      } else {
        pk = gnsClient.publicKeyLookupFromGuid(alias);
      }
      console.printString(alias + " public key is " + ByteUtils.toHex(pk.getEncoded()));
      console.printNewline();
    } catch (Exception e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
