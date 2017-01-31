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

import java.io.IOException;

import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Shows the help
 */
public class Help extends ConsoleCommand
{

  /**
   * Creates a new <code>Help</code> object
   * 
   * @param module the command is attached to
   */
  public Help(ConsoleModule module)
  {
    super(module);
  }

  /**
   * @throws java.io.IOException
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#parse(java.lang.String)
   */
  @Override
  public void parse(String commandText) throws IOException
  {
    module.help();
  }

  /**
   * @return the command name
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#getCommandName()
   */
  @Override
  public String getCommandName()
  {
    return "help"; //$NON-NLS-1$
  }

  /**
   * @return the command description
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#getCommandDescription()
   */
  @Override
  public String getCommandDescription()
  {
    return "Print this help message"; //$NON-NLS-1$
  }

  @Override
  public String getCommandParameters()
  {
    return "";
  }

  /**
   * @throws java.lang.Exception
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#execute(java.lang.String)
   */
  @Override
  public void execute(String commandText) throws Exception
  {
    this.parse(commandText);
  }

}
