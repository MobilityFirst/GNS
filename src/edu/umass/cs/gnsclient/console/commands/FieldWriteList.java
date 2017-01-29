
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.utils.StringUtil;
import org.json.JSONArray;

import java.util.StringTokenizer;


public class FieldWriteList extends ConsoleCommand
{


  public FieldWriteList(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Write a value in the given field of the target GUID (using the credential of the current GUID/alias). "
        + "The field is written as a list with the value as the single element. If the field doesn't exist it is created otherwise any previous value is overwritten.";
  }

  @Override
  public String getCommandName()
  {
    return "field_write_list";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias] field value";
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
      if (st.countTokens() == 2)
      {
        guid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 3)
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
      String value = st.nextToken();

      gnsClient.fieldReplaceOrCreateList(guid, field, new JSONArray().put(value), module.getCurrentGuid());
      console.printString("Value '" + value + "' written to field " + field + " for GUID " + guid);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
