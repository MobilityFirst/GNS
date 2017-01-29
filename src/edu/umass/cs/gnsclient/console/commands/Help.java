
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.console.ConsoleModule;

import java.io.IOException;


public class Help extends ConsoleCommand
{


  public Help(ConsoleModule module)
  {
    super(module);
  }


  public void parse(String commandText) throws IOException
  {
    module.help();
  }


  public String getCommandName()
  {
    return "help"; //$NON-NLS-1$
  }


  public String getCommandDescription()
  {
    return "Print this help message"; //$NON-NLS-1$
  }

  @Override
  public String getCommandParameters()
  {
    return "";
  }


  public void execute(String commandText) throws Exception
  {
    this.parse(commandText);
  }

}
