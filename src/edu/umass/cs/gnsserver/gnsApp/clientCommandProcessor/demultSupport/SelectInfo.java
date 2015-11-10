/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

/**
 * This class represents a data structure to store information
 * about queries (SELECT like lookup) transmitted by the local name
 * server.
 */

public class SelectInfo {

  private final int id;

  /**
   * Constructs a SelectInfo instance with the following parameters.
   *
   * @param id Query id
   */
  public SelectInfo(int id) {
    this.id = id;
  }

  /**
   * Returns the id.
   * 
   * @return the id
   */
  public int getId() {
    return id;
  }

}
