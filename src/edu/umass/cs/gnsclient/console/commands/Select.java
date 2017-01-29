
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.StringTokenizer;


public class Select extends ConsoleCommand
{


  public Select(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Returns all records that have a field that contains the given value";
  }

  @Override
  public String getCommandName()
  {
    return "select";
  }

  @Override
  public String getCommandParameters()
  {
    return "field value";
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
      GNSClientCommands gnsClient = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 2)
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String field = st.nextToken();

      String value = st.nextToken();

      JSONArray result = gnsClient.select(field, value);
      console.printString(result.toString());
      console.printNewline();
//      for (int i = 0; i < result.length(); i++)
//      {
//        console.printString(result.getJSONObject(i).getString("GUID") + " -> "
//            + toPrettyRecordString(result.getJSONObject(i)));
//        console.printNewline();
//      }
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }

  private String toPrettyRecordString(JSONObject json) throws Exception
  {
    json.remove("GUID");
    return json.toString();
  }
}
