
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.StringUtil;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONObject;


public class Update extends ConsoleCommand {


  public Update(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Update the value of the target GUID using the JSON String (using the credential of the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "update";
  }

  @Override
  public String getCommandParameters() {
    return "[target_guid_or_alias] jsonString";
  }


  @Override
  public void execute(String commandText) throws Exception {
    if (!module.isCurrentGuidSetAndVerified()) {
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    GNSClientCommands gnsClient = module.getGnsClient();
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String guid;
      if (st.countTokens() == 1) {
        guid = module.getCurrentGuid().getGuid();
      } else if (st.countTokens() == 2) {
        guid = st.nextToken();
        if (!StringUtil.isValidGuidString(guid)) {
          // We probably have an alias, lookup the GUID
          guid = gnsClient.lookupGuid(guid);
        }
      } else {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String value = st.nextToken();
      JSONObject json = new JSONObject(value);
      

      gnsClient.update(guid, json, module.getCurrentGuid());
      console.printString("GUID " + guid + " has been updated using '" + json.toString());
      console.printNewline();
    } catch (IOException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    } catch (ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    } catch (JSONException e) {
      console.printString("Unable to parse JSON string: " + e + "\n");
    }
  }
}
