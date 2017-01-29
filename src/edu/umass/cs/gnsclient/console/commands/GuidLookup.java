
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;

import java.util.StringTokenizer;


public class GuidLookup extends ConsoleCommand
{


  public GuidLookup(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Lookup the GUID corresponding to an alias";
  }

  @Override
  public String getCommandName()
  {
    return "guid_lookup";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias";
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
      String alias = st.nextToken();

      GNSClientCommands gnsClient = module.getGnsClient();
      String value = gnsClient.lookupGuid(alias);
      console.printString(alias + " has GUID " + value);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
