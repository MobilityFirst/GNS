/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.database;

import java.util.logging.Logger;

/**
 *
 * @author westy
 */
public class DatabaseConfig {

  private static final Logger LOG = Logger.getLogger(DatabaseConfig.class.getName());

  /**
   * @return Logger used by most of the client support package.
   */
  public static final Logger getLogger() {
    return LOG;
  }

}
