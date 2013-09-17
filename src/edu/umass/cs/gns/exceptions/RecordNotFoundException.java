/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.exceptions;

import edu.umass.cs.gns.exceptions.GnsException;
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
