
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;


public class AccountDelete extends ConsoleCommand
{


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
      GNSClientCommands gnsClient = module.getGnsClient();
      try
      {
        gnsClient.lookupGuid(aliasName);
      }
      catch (Exception expected)
      {
        printString("Alias " + aliasName + " doesn't exist.\n");
        return;
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);
      if (myGuid == null)
      {
        printString("Unable to retrieve GUID for alias " + aliasName + "\n");
        return;
      }

      gnsClient.accountGuidRemove(myGuid);
      KeyPairUtils.removeKeyPair(module.getGnsInstance(), aliasName);
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
