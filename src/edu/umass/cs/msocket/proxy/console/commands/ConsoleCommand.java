/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket.proxy.console.commands;

import jline.ConsoleReader;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * This class defines a ConsoleCommand
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class ConsoleCommand implements Comparable<ConsoleCommand>
{
  protected ConsoleModule module;
  protected ConsoleReader console;

  /**
   * Creates a new <code>ConsoleCommand</code> object
   * 
   * @param reader the console reader we are attached to
   */
  public ConsoleCommand(ConsoleModule module)
  {
    this.module = module;
    this.console = module.getConsole();
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(ConsoleCommand c)
  {
    return getCommandName().compareTo(c.getCommandName());
  }

  /**
   * Parse the text of the command
   * 
   * @param commandText the command text
   * @throws Exception if connection with the mbean server is lost or command
   *           does not have the proper format
   */
  public abstract void parse(String commandText) throws Exception;

  /**
   * Check if the JMX connection is still valid. Otherwise reconnect.
   * 
   * @param commandText the parameters to execute the command with
   * @throws Exception if fails
   */
  public void execute(String commandText) throws Exception
  {
    if (module.getGnsClient() == null)
    {
      console.printString("Not connected to the GNS. Cannot execute command. Use connect or help for instructions.\n");
      return;
    }
    parse(commandText);
  }

  /**
   * Get the name of the command
   * 
   * @return <code>String</code> of the command name
   */
  public abstract String getCommandName();

  /**
   * Return a <code>String</code> description of the parameters of this command.
   * 
   * @return <code>String</code> like &lt;driverPathName&gt;
   */
  public abstract String getCommandParameters();

  /**
   * Get the description of the command
   * 
   * @return <code>String</code> of the command description
   */
  public abstract String getCommandDescription();

  /**
   * Get the usage of the command.
   * 
   * @return <code>String</code> of the command usage ()
   */
  public String getUsage()
  {
    String usage = "Usage: " + getCommandName() + getCommandParameters() + "\n   " + getCommandDescription();
    return usage;
  }
}
