
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.StringUtil;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.PublicKey;
import java.util.StringTokenizer;


public class KeyLookup extends ConsoleCommand {


  public KeyLookup(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Lookup the public key of an alias or GUID (defaults to the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "key_lookup";
  }

  @Override
  public String getCommandParameters() {
    return "[alias_or_guid]";
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String alias;
      switch (st.countTokens()) {
        case 0:
          alias = module.getCurrentGuid().getEntityName();
          break;
        case 1:
          alias = st.nextToken();
          break;
        default:
          console.printString("Wrong number of arguments for this command.\n");
          return;
      }
      GNSClientCommands gnsClient = module.getGnsClient();
      PublicKey pk;
      if (!StringUtil.isValidGuidString(alias)) {
        pk = gnsClient.publicKeyLookupFromAlias(alias);
      } else {
        pk = gnsClient.publicKeyLookupFromGuid(alias);
      }
      console.printString(alias + " public key is " + DatatypeConverter.printHexBinary(pk.getEncoded()));
      //console.printString(alias + " public key is " + ByteUtils.toHex(pk.getEncoded()));
      console.printNewline();
    } catch (IOException | ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
