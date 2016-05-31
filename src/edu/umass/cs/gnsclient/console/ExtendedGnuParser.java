/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsclient.console;

import java.util.ListIterator;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author westy
 */
public class ExtendedGnuParser extends GnuParser {
  
  private boolean ignoreUnrecognizedOption;

  public ExtendedGnuParser(final boolean ignoreUnrecognizedOption) {
    this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
  }

  @Override
  protected void processOption(final String arg, final ListIterator iter) throws ParseException {
    boolean hasOption = getOptions().hasOption(arg);
    if (hasOption || !ignoreUnrecognizedOption) {
      super.processOption(arg, iter);
    }
  }
  
}
