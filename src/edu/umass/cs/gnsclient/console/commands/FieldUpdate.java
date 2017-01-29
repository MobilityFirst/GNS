
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.StringUtil;
import java.io.IOException;


public class FieldUpdate extends ConsoleCommand {


  public FieldUpdate(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Write a value in the given field of the target GUID (using the credential of the current GUID/alias), "
            + "if the field doesn't exist it is created otherwise any previous value is overwritten";
  }

  @Override
  public String getCommandName() {
    return "field_update";
  }

  @Override
  public String getCommandParameters() {
    return "[target_guid_or_alias] field value";
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
      if (st.countTokens() == 2) {
        guid = module.getCurrentGuid().getGuid();
      } else if (st.countTokens() == 3) {
        guid = st.nextToken();
        if (!StringUtil.isValidGuidString(guid)) {
          // We probably have an alias, lookup the GUID
          guid = gnsClient.lookupGuid(guid);
        }
      } else {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String field = st.nextToken();
      String value = st.nextToken();

      gnsClient.fieldUpdate(guid, field, value, module.getCurrentGuid());
      console.printString("Value '" + value + "' written to field " + field + " for GUID " + guid);
      console.printNewline();
    } catch (IOException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    } catch (ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
