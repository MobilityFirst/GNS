package edu.umass.cs.gns.nsdesign;

import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

/*** DONT USE ANY CLASS IN PACKAGE: edu.umass.cs.gns.nsdesign ***/


/**
 * Combines options given in config file and via command line arguments and starts a name server.
 * Options given via command line override those given in config file. Unlike previous implementation,
 * we will not use this class to store parameter values as static fields. Do not add any name server
 * parameters to this class.
 *
 * Created by abhigyan on 2/28/14.
 */
public class StartNameServer {


  /**
   * Returns a hash map with all options including options in config file and the command line arguments
   * @param args command line arguments given to JVM
   * @return hash map with KEY = parameter names, VALUE = values of parameters in String form
   * @throws IOException
   */
  public static HashMap<String, String> getParametersAsHashMap(String ... args) throws IOException {

    CommandLine parser = null;
    Options commandLineOptions = NSParameterNames.getAllOptions();

    try {
      parser =  new GnuParser().parse(commandLineOptions, args);
    } catch (ParseException e) {
      e.printStackTrace();
      printUsage(commandLineOptions);
      System.exit(1);
      e.printStackTrace();
    }

    if (parser.hasOption(NSParameterNames.HELP)) {
      printUsage(commandLineOptions);
      System.exit(1);
    }

    // load options given in config file in a java properties object
    Properties prop = new Properties();

    if (parser.hasOption(NSParameterNames.CONFIG_FILE))  {
      String value = parser.getOptionValue(NSParameterNames.CONFIG_FILE);
      File f = new File(value);
      if (f.exists() == false) {
        System.err.println("Config file not found:" + value);
        System.exit(2);
      }
      InputStream input = new FileInputStream(value);
      // load a properties file
      prop.load(input);

    }

    // create a hash map with all options including options in config file and the command line arguments
    HashMap<String, String> allValues = new HashMap<String, String>();

    // add options given in config file to hash map
    for (String propertyName: prop.stringPropertyNames()) {
      allValues.put(propertyName, prop.getProperty(propertyName));
    }

    // add options given via command line to hashmap. these options can override options given in config file.
    for (Option option: parser.getOptions()) {
      String argName = option.getOpt();
      String value = option.getValue();
      // if an option has a boolean value, the command line arguments do not say true/false for some of these options
      // if option name is given as argument on the command line, it means the value is true. therefore, the hashmap
      // will also assign the value true for these options.
      if (value == null) value = "true";
      allValues.put(argName, value);
//        System.out.println("adding: " + argName + "\t" + value);
    }
//      System.out.println("All values: " + allValues);

    return allValues;

  }

  /**
   * Prints command line usage
   */
  private static void printUsage(Options commandLineOptions) {
    new HelpFormatter().printHelp("StartNameServer", commandLineOptions);
  }

  /**
   *
   * Main method that starts the name server with the given command line options and config file options.
   *
   * @param args Command line arguments
   * @throws ParseException
   */
  public static void main(String[] args) throws IOException{

    HashMap<String, String> allValues = getParametersAsHashMap(args);
    // initialize config options.
    Config.initialize(allValues);
    int nodeID = Integer.parseInt(allValues.get(NSParameterNames.ID));
    String nodeConfigFile = allValues.get(NSParameterNames.NS_FILE);
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(nodeConfigFile, nodeID);

    new NameServer(nodeID, allValues, gnsNodeConfig);
  }

}
