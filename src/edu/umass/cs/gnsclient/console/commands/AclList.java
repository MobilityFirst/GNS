
package edu.umass.cs.gnsclient.console.commands;

import org.json.JSONArray;

import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;


public class AclList extends ConsoleCommand
{


  public AclList(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "List the current ACLs defined for the given field in the current GUID (using the credential of the current GUID/alias)";
  }

  @Override
  public String getCommandName()
  {
    return "acl_list";
  }

  @Override
  public String getCommandParameters()
  {
    return "field";
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

      String field = commandText.trim();

      // Read ACLs first (we just do the white lists are black lists are not
      // implemented yet)
      JSONArray read = gnsClient.aclGet(AclAccessType.READ_WHITELIST, module.getCurrentGuid(), field, module
          .getCurrentGuid().getGuid());
      console.printString("Read ACL: " + read.toString());
      console.printNewline();

      // Then write ACLs
      JSONArray write = gnsClient.aclGet(AclAccessType.WRITE_WHITELIST, module.getCurrentGuid(), field, module
          .getCurrentGuid().getGuid());
      console.printString("Write ACL: " + write.toString());
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
