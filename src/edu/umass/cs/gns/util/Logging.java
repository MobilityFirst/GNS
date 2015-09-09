/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.util;

import java.io.File;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author westy
 */
public class Logging {

  /**
   * The default level for writing to the screen.
   */
  public static Level DEFAULTCONSOLELEVEL = Level.INFO;

  /**
   * The default level for writing to a file.
   */
  public static Level DEFAULTFILELEVEL = Level.FINE;

  /**
   * The default file path for logging.
   */
  public static String DEFAULTLOGFILENAME = "log" + System.getProperty("file.separator") + "gns_log.xml";
  
  private static ConsoleHandler consoleHandler;

  /**
   *
   * @return
   */
  public static ConsoleHandler getConsoleHandler() {
    return consoleHandler;
  }

  /**
   * Sets up the logger using default values.
   *
   * @param logger
   */
  public static void setupLogger(Logger logger) {
    setupLogger(logger, DEFAULTCONSOLELEVEL.getName(), DEFAULTFILELEVEL.getName(), DEFAULTLOGFILENAME);
  }

  /**
   * Sets up the logger.
   *
   * @param logger
   * @param consoleLevelName
   * @param fileLevelName
   * @param logFilename
   */
  public static void setupLogger(Logger logger, String consoleLevelName, String fileLevelName, String logFilename) {
    File dir = new File(logFilename).getParentFile();
    if (!dir.exists()) {
      dir.mkdirs();
    }
    Level consoleLevel;
    Level fileLevel;

    try {
      consoleLevel = Level.parse(consoleLevelName);
    } catch (Exception e) {
      consoleLevel = DEFAULTCONSOLELEVEL;
    }
    try {
      fileLevel = Level.parse(fileLevelName);
    } catch (Exception e) {
      fileLevel = DEFAULTFILELEVEL;
    }
    
    // set the overall level to the less severe (more messages) of the two
    if (fileLevel.intValue() < consoleLevel.intValue()) {
      logger.setLevel(fileLevel);
    } else {
       logger.setLevel(consoleLevel);
    }

    logger.setUseParentHandlers(false);
    
    try {
      consoleHandler = new ConsoleHandler();
      // Use our one-line formatter
      consoleHandler.setFormatter(new LogFormatter());
      consoleHandler.setLevel(consoleLevel);
      logger.addHandler(consoleHandler);
    } catch (Exception e) {
      logger.warning("Unable to attach ConsoleHandler to logger!");
      e.printStackTrace();
    }

    try {
      Handler fh = new FileHandler(logFilename, 40000000, 45);
      fh.setLevel(fileLevel);
      logger.addHandler(fh);
    } catch (Exception e) {
      logger.warning("Unable to attach FileHandler to logger!");
      e.printStackTrace();
    }
  }
}
