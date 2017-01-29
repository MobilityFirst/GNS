
package edu.umass.cs.gnsclient.console.commands;

import java.util.List;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;


public class GuidList extends ConsoleCommand
{


  public GuidList(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "List the GUIDs stored locally for a given GNS";
  }

  @Override
  public String getCommandName()
  {
    return "guid_list";
  }

  @Override
  public String getCommandParameters()
  {
    return "[gnsHost:gnsPort]";
  }


  @Override
  public void execute(String commandText) throws Exception
  {
    // This command can be used without being connected to the GNS
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    if (module.getGnsClient() == null && commandText.isEmpty())
    {
      console.printString("You have to connect to a GNS or provide a GNS host:port parameter.\n");
      return;
    }

    String gnsName;
    commandText = commandText.trim();
    if (!commandText.isEmpty())
      gnsName = commandText;
    else
      gnsName = module.getGnsClient().getGNSProvider();

    // Lookup user preferences
    console.printString("GUIDs stored locally for GNS " + gnsName + ":\n");
    console.printString("Default GUID: ");
    GuidEntry guid = KeyPairUtils.getDefaultGuidEntry(gnsName);
    if (guid == null)
      console.printString("None");
    else
      console.printString(guid.toString());
    console.printNewline();
    List<GuidEntry> guids = KeyPairUtils.getAllGuids(gnsName);
    for (GuidEntry guidEntry : guids)
    {
      console.printString(guidEntry.toString());
      console.printNewline();
    }
  }
}
