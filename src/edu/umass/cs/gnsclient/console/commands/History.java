
package edu.umass.cs.gnsclient.console.commands;

import java.util.List;
import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.console.ConsoleModule;


public class History extends ConsoleCommand
{


  public History(ConsoleModule module)
  {
    super(module);
  }


  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }


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


  @Override
  public String getCommandName()
  {
    return "history"; //$NON-NLS-1$
  }


  @Override
  public String getCommandDescription()
  {
    return "Display history of commands for the console.";
  }


  @Override
  public String getCommandParameters()
  {
    return "[<command index>]"; //$NON-NLS-1$
  }
}