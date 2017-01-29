
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import java.util.StringTokenizer;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.console.ConsoleModule;


public class FieldCreateList extends ConsoleCommand
{


  public FieldCreateList(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Create a new field with an initial value for the current GUID/alias)";
  }

  @Override
  public String getCommandName()
  {
    return "field_create_list";
  }

  @Override
  public String getCommandParameters()
  {
    return "field_to_create [initial_value]";
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
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if ((st.countTokens() != 1) && (st.countTokens() != 2))
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String field = st.nextToken();
      String value = "";
      if (st.hasMoreTokens())
        value = st.nextToken();

      GNSClientCommands gnsClient = module.getGnsClient();
      gnsClient.fieldCreateList(module.getCurrentGuid().getGuid(), field, new JSONArray().put(value),
          module.getCurrentGuid());
      console.printString("New field " + field + " created with value '" + value + "'");
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
