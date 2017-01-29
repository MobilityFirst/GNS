
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.utils.StringUtil;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;


public class ActiveCodeSet extends ConsoleCommand
{


  public ActiveCodeSet(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Set active code for the target GUID and action (using the credential of the current GUID/alias), "
        + "if the callbacks don't exist, they are created, otherwise they are overwritten";
  }

  @Override
  public String getCommandName()
  {
    return "activecode_set";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias] action filename";
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
      String guid;
      if (st.countTokens() == 2)
      {
        guid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 3)
      {
        guid = st.nextToken();
        if (!StringUtil.isValidGuidString(guid))
        {
          // We probably have an alias, lookup the GUID
          guid = gnsClient.lookupGuid(guid);
        }
      }
      else
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      
      String action = st.nextToken();
      String filename = st.nextToken();
      byte[] code = Files.readAllBytes(Paths.get(filename));
     
      gnsClient.activeCodeSet(guid, action, code, module.getCurrentGuid());
      
      console.printString("Code in '" + filename + "' installed for GUID " + guid + "for action '" + action + "'");
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
