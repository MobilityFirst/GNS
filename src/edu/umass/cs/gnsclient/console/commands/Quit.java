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
 * Quit the CLI
 */
public class Quit extends ConsoleCommand
{

  /**
   * Creates a new <code>Quit</code> object
   * 
   * @param module the command is attached to
   */
  public Quit(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public void parse(String commandText) throws IOException
  {
    module.quit();
  }

  @Override
  public String getCommandName()
  {
    return "quit"; //$NON-NLS-1$
  }

  @Override
  public String getCommandDescription()
  {
    return "Quit the console"; //$NON-NLS-1$
  }

  @Override
  public String getCommandParameters()
  {
    return "";
  }

  @Override
  public void execute(String commandText) throws Exception
  {
    this.parse(commandText);
  }

}