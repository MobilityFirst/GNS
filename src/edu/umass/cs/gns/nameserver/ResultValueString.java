/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver;

import java.util.ArrayList;
import java.util.Collection;

// PLEASE DON'T DELETE THIS CLASS WITHOUT TALKING TO WESTY FIRST!

/**
 * This class is used to represent values in key / values for some of the "system" keys.
 * It differs from ResultValue in that the elements are Strings.
 *
 * @author westy
 */
public class ResultValueString extends ArrayList<String> {

  public ResultValueString() {
  }

  public ResultValueString(Collection<? extends String> clctn) {
    super(clctn);
  }
  
}
