
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.AclAccessType;
import org.json.JSONArray;

import java.util.StringTokenizer;


public class AclRemove extends ConsoleCommand
{


  public AclRemove(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Remove a GUID from the read or write ACL for the given field in the current GUID (using the credential of the current GUID/alias). "
        + "Use +ALL+ as the field name to set the ACL on all fields. "
        + "Use +ALL+ in the GUID field to make the field private and inaccessible to everyone.";
  }

  @Override
  public String getCommandName()
  {
    return "acl_remove";
  }

  @Override
  public String getCommandParameters()
  {
    return "field read|write guid";
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
      GNSClientCommands gnsClient = module.getGnsClient();
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 3)
      {
        console.printString("Wrong number of arguments");
        console.printNewline();
        return;
      }
      String field = st.nextToken();
      boolean isWrite = "write".equalsIgnoreCase(st.nextToken());
      String guid = st.nextToken();

      // Set ACL
      gnsClient.aclRemove(isWrite ? AclAccessType.WRITE_WHITELIST : AclAccessType.READ_WHITELIST, module.getCurrentGuid(),
          field, "+ALL+".equals(guid) ? null : guid);

      // Then read ACLs
      JSONArray write = gnsClient.aclGet(isWrite ? AclAccessType.WRITE_WHITELIST : AclAccessType.READ_WHITELIST,
          module.getCurrentGuid(), field, module.getCurrentGuid().getGuid());
      console.printString("New ACL is: " + write.toString());
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
