
package edu.umass.cs.gnscommon.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;


// This is used in all of the logging .properties files so don't delete it.
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