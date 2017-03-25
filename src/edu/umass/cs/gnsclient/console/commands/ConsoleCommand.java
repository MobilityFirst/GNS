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

import jline.ConsoleReader;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * This class defines a ConsoleCommand
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class ConsoleCommand implements Comparable<ConsoleCommand>
{
  /**
   * The module the command is attached to
   */
  protected ConsoleModule module;
  /**
   * The console to use to output messages
   */
  protected ConsoleReader console;

  /**
   * Creates a new <code>ConsoleCommand</code> object
   * 
   * @param module the console reader we are attached to
   */
  public ConsoleCommand(ConsoleModule module)
  {
    this.module = module;
    this.console = module.getConsole();
  }

  /**
   * Outputs a string on the console output (without carriage return)
   * 
   * @param string the string to print
   */
  public void printString(String string)
  {
    try
    {
      console.printString(string);
    }
    catch (IOException e)
    {
      GNSClientConfig.getLogger().warning("Problem printing string to console: " + e);
    }
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
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
   * Check if we are connected to the GNS before executing the command
   * 
   * @param commandText the parameters to execute the command with
   * @throws Exception if fails
   */
  public void execute(String commandText) throws Exception
  {
    if (module.getGnsClient() == null)
    {
      printString("Not connected to the GNS. Cannot execute command. Use gns_connect or help for instructions.\n");
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
  
  public void wrongArguments() throws IOException {
    console.printString("Wrong number of arguments; expected " + getCommandParameters());
    console.printNewline();
  }
}
