
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.utils.StringUtil;
import org.json.JSONArray;

import java.util.StringTokenizer;


public class GroupMemberList extends ConsoleCommand
{


  public GroupMemberList(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "List members of a group GUID";
  }

  @Override
  public String getCommandName()
  {
    return "group_member_list";
  }

  @Override
  public String getCommandParameters()
  {
    return "[group_guid_or_alias]";
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
    GNSClientCommands gnsClient = module.getGnsClient();
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String groupGuid;
      if (st.countTokens() == 0)
      {
        groupGuid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 1)
      {
        groupGuid = st.nextToken();
        if (!StringUtil.isValidGuidString(groupGuid))
        {
          // We probably have an alias, lookup the GUID
          groupGuid = gnsClient.lookupGuid(groupGuid);
        }
      }
      else
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }

      console.printString("Members in group " + groupGuid);
      console.printNewline();
      JSONArray members = gnsClient.groupGetMembers(groupGuid, module.getCurrentGuid());
      for (int i = 0; i < members.length(); i++)
      {
        console.printString(i + ": " + members.getString(i));
        console.printNewline();
      }
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
