/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author westy
 */
public class ResultValue extends ArrayList<String> {

  public ResultValue() {
  }

  public ResultValue(Collection<? extends String> clctn) {
    super(clctn);
  }
  
}
