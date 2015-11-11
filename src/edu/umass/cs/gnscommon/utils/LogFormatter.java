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
package edu.umass.cs.gnscommon.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Alter the default formatter to be less verbose.
 * 
 */
public class LogFormatter extends Formatter {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  @Override
  public String format(LogRecord record) {
    StringBuilder sb = new StringBuilder();
    
    String className = record.getSourceClassName();
    int index = className.lastIndexOf(".");
    String classOnly = className.substring(index + 1);

    sb.append(new Date(record.getMillis()));
    sb.append(" ");
    sb.append(record.getLevel().getLocalizedName());
    sb.append(" ");
    sb.append(classOnly);
    sb.append(".");
    sb.append(record.getSourceMethodName());
    sb.append(": ");
    sb.append(formatMessage(record));
    sb.append(LINE_SEPARATOR);

    if (record.getThrown() != null) {
      try {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        pw.close();
        sb.append(sw.toString());
      } catch (Exception ex) {
        // ignore
      }
    }

    return sb.toString();
  }
}