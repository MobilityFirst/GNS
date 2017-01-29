
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.utils.StringUtil;


public class FieldReadList extends ConsoleCommand
{


  public FieldReadList(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Read all the values of the given field in the target GUID (using the credential of the current GUID/alias)."
            + " Assumes the field is a list. Use in conjunction with field_write_list.";
  }

  @Override
  public String getCommandName()
  {
    return "field_read_list";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias] field_to_read";
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
    try
    {
      GNSClientCommands gnsClient = module.getGnsClient();

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

      JSONArray value = gnsClient.fieldReadArray(guid, field, module.getCurrentGuid());
      console.printString(value.toString());
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
