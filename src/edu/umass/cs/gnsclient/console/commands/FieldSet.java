
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.utils.StringUtil;

import java.util.StringTokenizer;


public class FieldSet extends ConsoleCommand
{


  public FieldSet(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Write a value at the given index in a field of the target GUID (using the credential of the current GUID/alias), "
            + "any previous value is overwritten."
            + " Assumes the field is a list. Use in conjunction with field_write_list.";
  }

  @Override
  public String getCommandName()
  {
    return "field_set";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias] index field value";
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
      if (st.countTokens() == 3)
      {
        guid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 4)
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
      String indexStr = st.nextToken();
      int index = -1;
      try
      {
        index = Integer.valueOf(indexStr);
      }
      catch (Exception e)
      {
        index = -1;
      }
      if (index < 0)
      {
        console.printString("Invalid index value.\n");
        return;
      }
      String field = st.nextToken();
      String value = st.nextToken();

      gnsClient.fieldSetElement(guid, field, value, index, module.getCurrentGuid());
      console.printString("Value '" + value + "' written at index " + index + " of field " + field + " for GUID "
          + guid);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
