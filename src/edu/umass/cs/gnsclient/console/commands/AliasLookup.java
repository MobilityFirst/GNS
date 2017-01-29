
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.GNSProtocol;


public class AliasLookup extends ConsoleCommand
{


  public AliasLookup(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Lookup the alias corresponding to an GUID";
  }

  @Override
  public String getCommandName()
  {
    return "alias_lookup";
  }

  @Override
  public String getCommandParameters()
  {
    return "GUID";
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
      String guid = st.nextToken();

      GNSClientCommands gnsClient = module.getGnsClient();
      JSONObject entityInfo = gnsClient.lookupGuidRecord(guid);
      String alias = entityInfo.getString(GNSProtocol.GUID_RECORD_NAME.toString());
      console.printString(guid + " has alias " + alias);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
