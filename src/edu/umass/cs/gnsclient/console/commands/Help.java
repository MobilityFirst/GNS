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
 * This class defines a Help
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk</a>
 * @version 1.0
 */
public class Help extends ConsoleCommand
{

  /**
   * Creates a new <code>Help.java</code> object
   * 
   * @param module the command is attached to
   */
  public Help(ConsoleModule module)
  {
    super(module);
  }

  /**
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws IOException
  {
    module.help();
  }

  /**
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "help"; //$NON-NLS-1$
  }

  /**
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#getCommandDescription()
   */
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
   * @see edu.umass.cs.gnsclient.console.commands.ConsoleCommand#execute(java.lang.String)
   */
  public void execute(String commandText) throws Exception
  {
    this.parse(commandText);
  }

}
