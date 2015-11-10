/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
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
