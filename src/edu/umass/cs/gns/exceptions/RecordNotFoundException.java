/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;
/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */

/**
 * Exception means that the record being accessed does not exist.
 * 
 * @author westy
 */

public class RecordNotFoundException extends GnsException{
  String name;

  /**
   * Create a RecordNotFoundException instance.
   * 
   * @param name
   */
  public RecordNotFoundException(String name) {
    this.name = name;
  }



}
