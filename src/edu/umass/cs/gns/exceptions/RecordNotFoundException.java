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
public class RecordNotFoundException extends GnsException{
  String name;
  public RecordNotFoundException(String name) {
    this.name = name;
  }



}
