
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.utils.StringUtil;


public class FieldDelete extends ConsoleCommand
{


  public FieldDelete(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Delete a the given field from the target GUID (using the credential of the current GUID/alias)";
  }

  @Override
  public String getCommandName()
  {
    return "field_delete";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias] field_to_remove";
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
    GNSClientCommands gnsClient = module.getGnsClient();
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String guid;
      if (st.countTokens() == 1)
      {
        guid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 2)
      {
        guid = st.nextToken();
        if (!StringUtil.isValidGuidString(guid))
        {
          // We probably have an alias, lookup the GUID
          guid = gnsClient.lookupGuid(guid);
        }
      }
      else
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String field = st.nextToken();

      gnsClient.fieldRemove(guid, field, module.getCurrentGuid());
      console.printString("Field " + field + " removed from GUID " + guid);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
