
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;


public class GuidDelete extends ConsoleCommand
{


  public GuidDelete(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Deletes alias from the GNS (also deletes local keypair information if any)";
  }

  @Override
  public String getCommandName()
  {
    return "guid_delete";
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
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 1)
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String aliasName = st.nextToken();

      if (!module.isSilent())
      {
        console.printString("Looking up alias " + aliasName + " certificates...\n");
      }
      GuidEntry guidToDelete = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

      if (guidToDelete == null)
      {
        console.printString("There is no local information for alias " + aliasName);
        console.printNewline();
        return;
      }

      module.getGnsClient().guidRemove(guidToDelete);
      console.printString("Alias " + aliasName + " removed from GNS.\n");
      KeyPairUtils.removeKeyPair(module.getGnsInstance(), aliasName);
      console.printString("Keys for " + aliasName + " removed from local repository.");
      console.printNewline();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      console.printString("Failed to remove alias or delete keys ( " + e + ")\n");
    }
  }
}
