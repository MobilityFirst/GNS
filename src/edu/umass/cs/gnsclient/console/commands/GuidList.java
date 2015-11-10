/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.console.commands;

import java.util.List;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * List GUIDs that are currently stored locally on the machine
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class GuidList extends ConsoleCommand
{

  /**
   * Creates a new <code>GuidList</code> object
   * 
   * @param module
   */
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

  /**
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#execute(java.lang.String)
   */
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
      gnsName = module.getGnsClient().getGnsRemoteHost() + ":" + module.getGnsClient().getGnsRemotePort();

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
