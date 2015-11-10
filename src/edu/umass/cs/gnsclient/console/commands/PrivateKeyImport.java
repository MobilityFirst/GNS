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

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.StringTokenizer;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Command that saves GUID/alias/Keypair information to a file
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class PrivateKeyImport extends ConsoleCommand
{

  /**
   * Creates a new <code>PrivateKeyImport</code> object
   * 
   * @param module
   */
  public PrivateKeyImport(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Loads a private key for guid from a file on disk";
  }

  @Override
  public String getCommandName()
  {
    return "private_key_import";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias path_and_filename";
  }

  /**
   * Override execute to not check for existing connectivity
   */
  @Override
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    UniversalTcpClient gnsClient = module.getGnsClient();
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 2)
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String aliasName = st.nextToken();
      String filename = st.nextToken();

      PrivateKey privateKey = KeyPairUtils.getPrivateKeyFromPKCS8File(filename);
      String guid = gnsClient.lookupGuid(aliasName);
      if (guid == null)
      {
        console.printString("Alias " + aliasName + " is not in the GNS");
        console.printNewline();
        return;
      }
      PublicKey publicKey = gnsClient.publicKeyLookupFromGuid(guid);
      GuidEntry guidEntry = new GuidEntry(aliasName, guid, publicKey, privateKey);

      KeyPairUtils.saveKeyPair(module.getGnsHostPort(), guidEntry.getEntityName(), guidEntry.getGuid(),
          new KeyPair(guidEntry.getPublicKey(), guidEntry.getPrivateKey()));
      console.printString("Private key for " + guidEntry.getEntityName() + " read from " + filename
          + " and saved in local preferences.");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      console.printString("Failed to save keys ( " + e + ")\n");
    }
  }
}
