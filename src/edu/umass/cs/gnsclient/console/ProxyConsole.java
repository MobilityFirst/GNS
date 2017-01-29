
package edu.umass.cs.gnsclient.console;

import java.io.IOException;
import java.io.PrintWriter;

import jline.ConsoleReader;


public class ProxyConsole
{


  public static void main(String[] args)
  {
    boolean silent = (args.length > 0 && "-silent".equalsIgnoreCase(args[0]));
    try
    {
      ConsoleReader consoleReader = new ConsoleReader(System.in,
          new PrintWriter(System.out, true));
      ConsoleModule module = new ConsoleModule(consoleReader);
      module.setSilent(silent);
      module.handlePrompt();
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(-1);
    }

  }

}
