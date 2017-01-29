
package edu.umass.cs.gnsclient.console.commands;

import java.io.IOException;

import edu.umass.cs.gnsclient.console.ConsoleModule;


public class Quit extends ConsoleCommand
{


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