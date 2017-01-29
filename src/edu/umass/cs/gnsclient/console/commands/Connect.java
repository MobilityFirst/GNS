
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;

import java.util.StringTokenizer;


public class Connect extends ConsoleCommand {


  public Connect(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Connects to the GNS";
  }

  @Override
  public String getCommandName() {
    return "gns_connect";
  }

  @Override
  public String getCommandParameters() {
    return "";
  }


  @Override
  public void execute(String commandText) throws Exception {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      StringTokenizer st = new StringTokenizer(commandText);
      if (st.countTokens() > 0) {
        console.printString("THE USE OF host, port and disableSSL is obsolete.\n"
                + "   See instead gigapaxos.properties.\n");
      }

      GNSClientCommands gnsClient;
      gnsClient = new GNSClientCommands();
      if (!module.isSilent()) {
        console.printString("Connected to GNS.\n");
      }
      // Set the global variable for other activities
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + "Connected to GNS>");
      module.setGnsClient(gnsClient);
    } catch (Exception e) {
      console.printString("Failed to connect to GNS ( " + e + ")\n");
      module.setGnsClient(null);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + "not connected>");
    }
  }
}
