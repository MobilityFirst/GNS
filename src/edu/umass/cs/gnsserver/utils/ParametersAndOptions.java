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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnsserver.main.GNS;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author westy
 */
public class ParametersAndOptions {

  /**
   * The config file option.
   */
  public static final String CONFIG_FILE = "configFile";

  /**
   * The help file option.
   */
  public static final String HELP = "help";

  /**
   * The header for printing help.
   */
  public static final String HELP_HEADER = "";

  /**
   * The footer for printing help.
   */
  public static final String HELP_FOOTER = "";

  /**
   * Returns a hash map with all options including options in config file and the command line arguments
   *
   * @param className
   * @param commandLineOptions
   * @param args command line arguments given to JVM
   * @return hash map with KEY = parameter names, VALUE = values of parameters in String form
   * @throws IOException
   */
  public static HashMap<String, String> getParametersAsHashMap(String className, Options commandLineOptions, 
          String... args) throws IOException {
    CommandLine parser = null;
    try {
      parser = new GnuParser().parse(commandLineOptions, args);
    } catch (ParseException e) {
      System.err.println("Problem parsing command line:" + e);
      printUsage(className, commandLineOptions);
      System.exit(1);
    }

    if (parser.hasOption(HELP)) {
      printUsage(className, commandLineOptions);
      System.exit(0);
    }

    // load options given in config file in a java properties object
    Properties prop = new Properties();

    if (parser.hasOption(CONFIG_FILE)) {
      String value = parser.getOptionValue(CONFIG_FILE);
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
    for (String propertyName : prop.stringPropertyNames()) {
      allValues.put(propertyName, prop.getProperty(propertyName));
    }

    // add options given via command line to hashmap. these options can override options given in config file.
    for (Option option : parser.getOptions()) {
      String argName = option.getOpt();
      String value = option.getValue();
      // if an option has a boolean value, the command line arguments do not say true/false for some of these options
      // if option name is given as argument on the command line, it means the value is true. therefore, the hashmap
      // will also assign the value true for these options.
      if (value == null) {
        value = "true";
      }
      allValues.put(argName, value);
    }

    return allValues;
  }

  /**
   * Returns true if the option is true. Looks
   * for strings like true, and not false.
   * 
   * @param key
   * @param options
   * @return true if the option is true
   */
  public static boolean isOptionTrue(String key, Map<String, String> options) {
    String value;
    return (value = options.get(key)) != null
            && !"false".equals(value)
            && ("true".equals(value)
            || "True".equals(value)
            || "TRUE".equals(value));
  }

  /**
   * Prints the usage string.
   * 
   * @param className
   * @param commandLineOptions
   */
  public static void printUsage(String className, Options commandLineOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(135);
    helpFormatter.printHelp("java -cp GNS.jar " + className, HELP_HEADER, commandLineOptions,
            HELP_FOOTER);
  }
  
  /**
   * Shows all the options on the console.
   * 
   * @param options
   */
  public static void printOptions(Map<String, String> options) {
    StringBuilder result = new StringBuilder();
    TreeMap<String, String> tree = new TreeMap<String, String>(options);
    for (Entry<String,String> entry : tree.entrySet()) {
      result.append(entry.getKey());
      result.append(" => ");
      result.append(entry.getValue());
      result.append("\n");
    }
    System.out.print(result.toString());
  }

  /**
   * Returns the complete set of options.
   * 
   * @return the options
   */
  public static Options getAllOptions() {
    Option help = new Option(HELP, "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option debug = new Option("debug", "Node ID");

    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);

    return commandLineOptions;
  }

  /**
   * The main routine. For testing.
   * 
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    args = new String[]{"-configFile", GNS.WESTY_GNS_DIR_PATH + "/conf/ec2_small/ns.properties"};
    Map<String, String> options
            = ParametersAndOptions.getParametersAsHashMap(ParametersAndOptions.class.getCanonicalName(),
                    getAllOptions(), args);
    printOptions(options);
    System.out.println("debug is " + isOptionTrue("debug", options));
    System.out.println("debugAPP is " + isOptionTrue("debugAPP", options));
  }

}
