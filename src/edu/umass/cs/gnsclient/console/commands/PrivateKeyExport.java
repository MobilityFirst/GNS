
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;


public class PrivateKeyExport extends ConsoleCommand {


  public PrivateKeyExport(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Saves private key for guid into a file on disk";
  }

  @Override
  public String getCommandName() {
    return "private_key_export";
  }

  @Override
  public String getCommandParameters() {
    return "alias path_and_filename";
  }


  @Override
  public void execute(String commandText) throws Exception {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 2) {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String aliasName = st.nextToken();
      String filename = st.nextToken();

      if (!module.isSilent()) {
        console.printString("Looking up alias " + aliasName + " GUID and certificates...\n");
      }
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

      if (myGuid == null) {
        console.printString("You do not have the private key for alias " + aliasName);
        console.printNewline();
        return;
      }

      if (KeyPairUtils.writePrivateKeyToPKCS8File(myGuid.getPrivateKey(), filename)) {
        console.printString("Private key for " + aliasName + " stored in " + filename + " in "
                + myGuid.getPrivateKey().getFormat() + " format.");
        console.printNewline();
        return;
      }
    } catch (Exception e) {
      e.printStackTrace();
      console.printString("Failed to save keys ( " + e + ")\n");
    }
    console.printString("Failed to save keys \n");
  }
}
