
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.utils.StringUtil;
import org.json.JSONObject;

import java.util.StringTokenizer;


public class Read extends ConsoleCommand {


  public Read(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Read the entire JSON Object record of target GUID (defaults to the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "read";
  }

  @Override
  public String getCommandParameters() {
    return "[target_guid_or_alias]";
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
    try {
      GNSClientCommands gnsClient = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      String guid;
      if (st.countTokens() == 0) {
        guid = module.getCurrentGuid().getGuid();
      } else if (st.countTokens() == 1) {
        guid = st.nextToken();
        if (!StringUtil.isValidGuidString(guid)) {
          // We probably have an alias, lookup the GUID
          guid = gnsClient.lookupGuid(guid);
        }
      } else {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }

      JSONObject value = gnsClient.read(guid, module.getCurrentGuid());
      console.printString(value.toString());
      console.printNewline();
    } catch (Exception e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
