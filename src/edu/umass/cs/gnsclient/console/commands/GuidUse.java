
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;


public class GuidUse extends ConsoleCommand
{


  public GuidUse(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Use a different alias/GUID to execute GNS commands.";
  }

  @Override
  public String getCommandName()
  {
    return "guid_use";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias";
  }


  @Override
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    try
    {
      String aliasName = commandText.trim();
      GNSClientCommands gnsClient = module.getGnsClient();

      try
      {
        gnsClient.lookupGuid(aliasName);
      }
      catch (Exception expected)
      {
        console.printString("Alias " + aliasName + " is not registered in the GNS");
        console.printNewline();
        return;
      }

      if (!module.isSilent())
        console.printString("Looking up alias " + aliasName + " GUID and certificates...\n");
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

      if (myGuid == null)
      {
        console.printString("You do not have the private key for alias " + aliasName);
        console.printNewline();
        return;
      }

      // Success, let's update the console prompt with the new alias name
      module.setCurrentGuidAndCheckForVerified(myGuid);
      console.printString("Current GUID set to " + myGuid);
      console.printNewline();
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + aliasName + ">");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      console.printString("Failed to access the GNS ( " + e + ")\n");
    }
  }
}
