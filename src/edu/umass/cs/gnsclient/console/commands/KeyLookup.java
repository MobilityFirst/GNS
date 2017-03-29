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
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.StringUtil;
import java.io.IOException;
import javax.xml.bind.DatatypeConverter;

/**
 * Lookup a Public Key for an alias or GUID
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
      switch (st.countTokens()) {
        case 0:
          if (!module.isCurrentGuidSetAndVerified()) {
            return;
          } else {
            alias = module.getCurrentGuid().getEntityName();
          }
          break;
        case 1:
          alias = st.nextToken();
          break;
        default:
          wrongArguments();
          return;
      }
      GNSClientCommands gnsClient = module.getGnsClient();
      PublicKey pk;
      if (!StringUtil.isValidGuidString(alias)) {
        pk = gnsClient.publicKeyLookupFromAlias(alias);
      } else {
        pk = gnsClient.publicKeyLookupFromGuid(alias);
      }
      console.printString(alias + " public key is " + DatatypeConverter.printHexBinary(pk.getEncoded()));
      //console.printString(alias + " public key is " + ByteUtils.toHex(pk.getEncoded()));
      console.printNewline();
    } catch (IOException | ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
