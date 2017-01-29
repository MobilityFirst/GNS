
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.VerificationException;

import java.util.StringTokenizer;


public class AccountVerify extends ConsoleCommand {


  public AccountVerify(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Verify the creation of an account";
  }

  @Override
  public String getCommandName() {
    return "account_verify";
  }

  @Override
  public String getCommandParameters() {
    return "alias code";
  }


  @Override
  public void execute(String commandText) throws Exception {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      GNSClientCommands client = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 2) {
        printString("Wrong number of arguments for this command.\n");
        return;
      }
      String alias = st.nextToken();
      GuidEntry guid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), alias);
      if (guid == null) {
        printString("Unable to retrieve GUID for alias " + alias + "\n");
        return;
      }

      String code = st.nextToken();

      try {
        if (client.accountGuidVerify(guid, code).startsWith(GNSProtocol.OK_RESPONSE.toString())) {
          printString("Account verified.\n");
          module.setAccountVerified(true);
          return;
        }
        // this happens if it was already verified, but we didn't notice
      } catch (VerificationException e) {
        module.setAccountVerified(true);
        printString("Account already verified"+"\n");
        return;
      } catch (Exception e) {
        printString("Account not verified: " + e + "\n");
      }
    } catch (Exception e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }

    module.setAccountVerified(false);
  }
}
