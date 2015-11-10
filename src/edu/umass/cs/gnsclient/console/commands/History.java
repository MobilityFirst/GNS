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
import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * This class defines a History
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class History extends ConsoleCommand
{

  /**
   * Creates a new <code>History</code> object
   * 
   * @param module module that owns this commands
   */
  public History(ConsoleModule module)
  {
    super(module);
  }

  /**
   * Override execute to not check for existing connectivity
   */
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  /**
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    List<String> list = module.getHistory();
    StringTokenizer st = new StringTokenizer(commandText);
    if (st.countTokens() == 0)
    {
      for (int i = 0; i < list.size(); i++)
      {
        Object o = list.get(i);
        console.printString("" + i + "\t" + o + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
      }
    }
    else
    {
      int line = Integer.parseInt(st.nextToken());
      console.printString(list.get(line) + "\n");
      module.handleCommandLine(list.get(line), module.getHashCommands());
    }
  }

  /**
   * @see edu.umass.cs.gnsclient.console.commands.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  @Override
  public String getCommandName()
  {
    return "history"; //$NON-NLS-1$
  }

  /**
   * @see edu.umass.cs.gnsclient.ConsoleCommand#getCommandDescription()
   */
  @Override
  public String getCommandDescription()
  {
    return "Display history of commands for the console.";
  }

  /**
   * @see edu.umass.cs.gnsclient.ConsoleCommand#getCommandParameters()
   */
  @Override
  public String getCommandParameters()
  {
    return "[<command index>]"; //$NON-NLS-1$
  }
}