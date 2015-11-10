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

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Command that creates a new proxy group
 * 
 * @author Westy
 * @version 1.0
 */
public class AccountDelete extends ConsoleCommand
{

  /**
   * Creates a new <code>AccountDelete</code> object
   * 
   * @param module
   */
  public AccountDelete(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Delete an account with a GUID.";
  }

  @Override
  public String getCommandName()
  {
    return "account_delete";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias";
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    String aliasName = commandText.trim();
    try
    {
      UniversalTcpClient gnsClient = module.getGnsClient();
      try
      {
        gnsClient.lookupGuid(aliasName);
      }
      catch (Exception expected)
      {
        printString("Alias " + aliasName + " doesn't exist.\n");
        return;
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsHostPort(), aliasName);
      if (myGuid == null)
      {
        printString("Unable to retrieve GUID for alias " + aliasName + "\n");
        return;
      }

      gnsClient.accountGuidRemove(myGuid);
      KeyPairUtils.removeKeyPair(module.getGnsHostPort(), aliasName);
      module.setCurrentGuidAndCheckForVerified(null);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + ">");
      if (!module.isSilent())
      {
        printString("Removed account GUID " + myGuid.getGuid() + "\n");
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      printString("Failed to delete guid ( " + e + ")\n");
    }
  }
}
