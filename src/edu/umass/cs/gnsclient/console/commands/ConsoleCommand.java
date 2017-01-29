
package edu.umass.cs.gnsclient.console.commands;

import java.io.IOException;

import jline.ConsoleReader;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.console.ConsoleModule;


public abstract class ConsoleCommand implements Comparable<ConsoleCommand>
{

  protected ConsoleModule module;

  protected ConsoleReader console;


  public ConsoleCommand(ConsoleModule module)
  {
    this.module = module;
    this.console = module.getConsole();
  }


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


  @Override
  public int compareTo(ConsoleCommand c)
  {
    return getCommandName().compareTo(c.getCommandName());
  }


  public abstract void parse(String commandText) throws Exception;


  public void execute(String commandText) throws Exception
  {
    if (module.getGnsClient() == null)
    {
      printString("Not connected to the GNS. Cannot execute command. Use gns_connect or help for instructions.\n");
      return;
    }
    parse(commandText);
  }


  public abstract String getCommandName();


  public abstract String getCommandParameters();


  public abstract String getCommandDescription();


  public String getUsage()
  {
    String usage = "Usage: " + getCommandName() + getCommandParameters() + "\n   " + getCommandDescription();
    return usage;
  }
}
