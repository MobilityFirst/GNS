
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;

import java.security.PublicKey;


public class GuidCreate extends ConsoleCommand
{


  public GuidCreate(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Create a new GUID, associate an alias and register it in the GNS.";
  }

  @Override
  public String getCommandName()
  {
    return "guid_create";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias";
  }


  @Override
  public void execute(String commandText) throws Exception
  {
    if (!module.isCurrentGuidSetAndVerified())
    {
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    GuidEntry accountGuid = module.getCurrentGuid();
    String aliasName = commandText.trim();
    try
    {
      GNSClientCommands gnsClient = module.getGnsClient();

      try
      {
        gnsClient.lookupGuid(aliasName);
        printString("Alias " + aliasName + " already exists.\n");
        return;
      }
      catch (Exception expected)
      {
        // The alias does not exists, that's good, let's create it
      }

      if (!module.isSilent())
      {
        printString("Looking for alias " + aliasName + " GUID and certificates...\n");
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

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
            KeyPairUtils.removeKeyPair(module.getGnsInstance(), aliasName);
          }
        }
        catch (InvalidGuidException e)
        {
          KeyPairUtils.removeKeyPair(module.getGnsInstance(), aliasName);
        }
      }

      if (!module.isSilent())
      {
        printString("Generating new GUID and keys for account " + accountGuid.getEntityName() + " \n");
      }
      myGuid = gnsClient.guidCreate(accountGuid, aliasName);

      if (!module.isSilent())
      {
        printString("Created GUID " + myGuid.getGuid() + "\n");
      }
      if (module.getCurrentGuid() == null)
      {
        module.setCurrentGuidAndCheckForVerified(myGuid);
        module.setPromptString(ConsoleModule.CONSOLE_PROMPT + aliasName + ">");
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      printString("Failed to create new guid ( " + e + ")\n");
    }
  }
}
