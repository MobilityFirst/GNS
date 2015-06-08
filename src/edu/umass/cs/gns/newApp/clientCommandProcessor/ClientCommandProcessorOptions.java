/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.HELP;
import static edu.umass.cs.gns.util.ParametersAndOptions.CONFIG_FILE;

/**
 * The command line options for ClientCommandProcessor.
 * 
 * @author westy
 */
public class ClientCommandProcessorOptions {

  // If you change this list, change it below in getAllOptions as well.
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String NS_FILE = "nsfile";
  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";
  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";
  public static final String DNS_GNS_ONLY = "dnsGnsOnly";
  public static final String DNS_ONLY = "dnsOnly";
  public static final String DEBUG = "debug";
  public static final String GNS_SERVER_IP = "gnsServerIP";
  public static final String AR_ID = "activeReplicaID";

  public static Options getAllOptions() {
    Option help = new Option(HELP, "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option nsFile = new Option(NS_FILE, true, "File with node configuration of all name servers");
    Option host = new Option(HOST, true, "Host");
    Option port = new Option(PORT, true, "Port");
    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "Verbosity level of log file. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option debug = new Option(DEBUG, "Enables debugging output");
    Option dnsGnsOnly = new Option(DNS_GNS_ONLY, "With this option DNS server only does lookup in GNS server.");
    Option dnsOnly = new Option(DNS_ONLY, "With this option name server forwards requests to DNS and GNS servers.");
    Option gnsServerIP = new Option(GNS_SERVER_IP, "gns server to use");
    Option activeReplicaID = new Option(AR_ID, true, "id of the corresponding active replica");
    
    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(host);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(dnsGnsOnly);
    commandLineOptions.addOption(dnsOnly);
    commandLineOptions.addOption(gnsServerIP);
    commandLineOptions.addOption(debug);
    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(activeReplicaID);

    return commandLineOptions;
  }

 
}
