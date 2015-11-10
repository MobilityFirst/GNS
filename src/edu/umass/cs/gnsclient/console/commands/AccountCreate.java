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

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnsclient.exceptions.GnsInvalidGuidException;
import java.util.StringTokenizer;

/**
 * Command that creates a new proxy group
 * 
 * @author Westy
 * @version 1.0
 */
public class AccountCreate extends ConsoleCommand
{

  /**
   * Creates a new <code>AccountCreate</code> object
   * 
   * @param module
   */
  public AccountCreate(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Create a new account which includes a GUID, associates it with the alias and register it in the GNS.";
  }

  @Override
  public String getCommandName()
  {
    return "account_create";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias password";
  }

  @Override
  public void parse(String commandText) throws Exception
  {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if ((st.countTokens() != 2))
      {
        console.printString("Wrong number of arguments for this command. Requires alias and password\n");
        return;
      }
      String aliasName = st.nextToken();
      String password = st.nextToken();

    try
    {
      UniversalTcpClient gnsClient = module.getGnsClient();

      try
      {
        gnsClient.lookupGuid(aliasName);
        if (!module.isSilent())
        {
          printString("Alias " + aliasName + " already exists.\n");
        }
        return;
      }
      catch (Exception expected)
      {
        // The alias does not exists, that's good, let's create it
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsHostPort(), aliasName);
      if (myGuid != null)
      {
        try
        {
          PublicKey pk = gnsClient.publicKeyLookupFromGuid(myGuid.getGuid());
          if (myGuid.getPublicKey().equals(pk))
          { // We already have the key but the alias is missing in the GNS,
            // re-add the alias
            printString("Alias info found locally but missing in GNS, re-adding alias to the GNS\n");
            gnsClient.guidCreate(myGuid, aliasName);
          }
          else
          {
            printString("Old certificates found locally and not matching key in GNS, deleting local keys\n");
            KeyPairUtils.removeKeyPair(module.getGnsHostPort(), aliasName);
          }
        }
        catch (GnsInvalidGuidException e)
        {
          KeyPairUtils.removeKeyPair(module.getGnsHostPort(), aliasName);
        }
      }
      myGuid = gnsClient.accountGuidCreate(aliasName, password);

      if (!module.isSilent())
      {
        printString("Created an account with GUID " + myGuid.getGuid() + ". An email has been sent to "
            + myGuid.getEntityName() + " with instructions on how to verify the new account.\n");
      }
      if (module.getCurrentGuid() == null)
      {
        module.setCurrentGuidAndCheckForVerified(myGuid);
        if (KeyPairUtils.getDefaultGuidEntry(module.getGnsHostPort()) == null)
        {
          KeyPairUtils.setDefaultGuidEntry(module.getGnsHostPort(), aliasName);
          module.printString(aliasName + " saved as default GUID for GNS " + module.getGnsHostPort() + "\n");
        }
        module.setPromptString(ConsoleModule.CONSOLE_PROMPT + module.getGnsHostPort() + "|" + aliasName + ">");
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      printString("Failed to create new guid ( " + e + ")\n");
    }
  }
}
