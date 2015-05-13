/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import static edu.umass.cs.gns.clientsupport.Defs.HELP;
import static edu.umass.cs.gns.util.ParametersAndOptions.CONFIG_FILE;

/**
 * The command line options for LocalNameServer.
 * 
 * @author westy
 */
public class LocalNameServerOptions {

  // If you change this list, change it below in getAllOptions as well.
  public static final String NS_FILE = "nsfile";
  public static final String PORT = "port";
  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";
  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";
  public static final String DEBUG = "debug";

  public static Options getAllOptions() {
    Option help = new Option(HELP, "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option nsFile = new Option(NS_FILE, true, "File with node configuration of all name servers");
    Option port = new Option(PORT, true, "Port");
    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "Verbosity level of log file. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option debug = new Option(DEBUG, "Enables debugging output");
    
    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(debug);
    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);

    return commandLineOptions;
  }

 
}
