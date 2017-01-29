
package edu.umass.cs.gnscommon.utils;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class Format {


  public final static ThreadLocal<DecimalFormat> formatTime
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("###.##");
    }
  };


  public static Number parseTime(String text) throws ParseException {
    return formatTime.get().parse(text);
  }


  public static String formatTime(double x) {
    return formatTime.get().format(x);
  }

  public final static ThreadLocal<DecimalFormat> formatLatLong
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("####.00000");
    }
  };


  public static Number parseLatLong(String text) throws ParseException {
    return formatLatLong.get().parse(text);
  }


  public static String formatLatLong(double x) {
    return formatLatLong.get().format(x);
  }

  public final static ThreadLocal<DecimalFormat> formatUtility
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("#.####");
    }
  };


  public static Number parseUtility(String text) throws ParseException {
    return formatUtility.get().parse(text);
  }


  public static String formatUtility(double x) {
    return formatUtility.get().format(x);
  }

  public final static ThreadLocal<DecimalFormat> formatdBZ
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("###.#");
    }
  };


  public static Number parsedBZ(String text) throws ParseException {
    return formatdBZ.get().parse(text);
  }


  public static String formatdBZ(double x) {
    return formatdBZ.get().format(x);
  }

  public final static ThreadLocal<DecimalFormat> formatFloatLong
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("#####.######");
    }
  };


  public static Number parseFloatLong(String text) throws ParseException {
    return formatFloatLong.get().parse(text);
  }


  public static String formatFloatLong(double x) {
    return formatFloatLong.get().format(x);
  }

  public final static ThreadLocal<DecimalFormat> formatFloat
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("####.###");
    }
  };


  public static Number parseFloat(String text) throws ParseException {
    return formatFloat.get().parse(text);
  }


  public static String formatFloat(double x) {
    return formatFloat.get().format(x);
  }


  public final static ThreadLocal<DecimalFormat> formatFloat2
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("#.0");
    }
  };


  public static Number parseFloat2(String text) throws ParseException {
    return formatFloat2.get().parse(text);
  }


  public static String formatFloat2(double x) {
    return formatFloat2.get().format(x);
  }


  public final static ThreadLocal<DecimalFormat> formatDistance
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("#0.0");
    }
  };


  public static Number parseDistance(String text) throws ParseException {
    return formatDistance.get().parse(text);
  }


  public static String formatDistance(double x) {
    return formatDistance.get().format(x);
  }

  public final static ThreadLocal<SimpleDateFormat> formatPrettyDate
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy.MM.dd H:mm:ss zzz");
    }
  };


  public static Date parsePrettyDate(String text) throws ParseException {
    return formatPrettyDate.get().parse(text);
  }


  public static String formatPrettyDate(Date date) {
    return formatPrettyDate.get().format(date);
  }

  public final static ThreadLocal<SimpleDateFormat> formatPrettyDateUTC
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("yyyy.MM.dd H:mm:ss zzz");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static String formatPrettyDateUTC(Date date) {
    return formatPrettyDateUTC.get().format(date);
  }

  public final static ThreadLocal<SimpleDateFormat> formatDate
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyyMMddHHmmss");
    }
  };


  public static Date parseDate(String text) throws ParseException {
    return formatDate.get().parse(text);
  }


  public static String formatDate(Date date) {
    return formatDate.get().format(date);
  }

  public final static ThreadLocal<SimpleDateFormat> formatDateUTC
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("yyyyMMddHHmmss");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static String formatDateUTC(Date date) {
    return formatDateUTC.get().format(date);
  }


  public static Date parseDateUTC(String text) throws ParseException {
    return formatDateUTC.get().parse(text);
  }


  public static Date parseDateUTCOrNull(String text) {
    try {
      return formatDateUTC.get().parse(text);
    } catch (Exception e) {
      return null;
    }
  }

  public final static ThreadLocal<SimpleDateFormat> formatShortDate
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("dd/MM/yy HH:mm:ss");
    }
  };


  public static Date parseShortDate(String text) throws ParseException {
    return formatShortDate.get().parse(text);
  }


  public static String formatShortDate(Date date) {
    return formatShortDate.get().format(date);
  }

  public final static ThreadLocal<SimpleDateFormat> formatShortDateUTC
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static Date parseShortDateUTC(String text) throws ParseException {
    return formatShortDateUTC.get().parse(text);
  }


  public static String formatShortDateUTC(Date date) {
    return formatShortDateUTC.get().format(date);
  }

  public final static ThreadLocal<SimpleDateFormat> formatDateTimeOnly
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("HH:mm:ss zzz");
    }
  };


  public static Date parseDateTimeOnly(String text) throws ParseException {
    return formatDateTimeOnly.get().parse(text);
  }


  public static String formatDateTimeOnly(Date date) {
    return formatDateTimeOnly.get().format(date);
  }
  //


  public final static ThreadLocal<SimpleDateFormat> formatDateTimeOnlyUTC
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("HH:mm:ss");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static Date parseDateTimeOnlyUTC(String text) throws ParseException {
    return formatDateTimeOnlyUTC.get().parse(text);
  }


  public static String formatDateTimeOnlyUTC(Date date) {
    return formatDateTimeOnlyUTC.get().format(date);
  }



  public static String formatDualDate(Date date) {
    return formatPrettyDateUTC.get().format(date) + " [" + formatDateTimeOnly.get().format(date) + "]";
  }


  public final static ThreadLocal<SimpleDateFormat> formatDateTimeOnlyMilleUTC
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("HH:mm:ss.SSS");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static Date parseDateTimeOnlyMilleUTC(String text) throws ParseException {
    return formatDateTimeOnlyMilleUTC.get().parse(text);
  }


  public static String formatDateTimeOnlyMilleUTC(Date date) {
    return formatDateTimeOnlyMilleUTC.get().format(date);
  }


  public final static ThreadLocal<SimpleDateFormat> formatLogDate
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static Date parseLogDate(String text) throws ParseException {
    return formatLogDate.get().parse(text);
  }


  public static String formatLogDate(Date date) {
    return formatLogDate.get().format(date);
  }

  public final static ThreadLocal<SimpleDateFormat> formatMYSQLDate
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static Date parseMYSQLDate(String text) throws ParseException {
    return formatMYSQLDate.get().parse(text);
  }


  public static String formatMYSQLDate(Date date) {
    return formatMYSQLDate.get().format(date);
  }

  public final static ThreadLocal<SimpleDateFormat> formatFilenameDate
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static Date parseFilenameDate(String text) throws ParseException {
    return formatFilenameDate.get().parse(text);
  }


  public static String formatFilenameDate(Date date) {
    return formatFilenameDate.get().format(date);
  }


  public final static ThreadLocal<DecimalFormat> formatQ
          = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("0.00");
    }
  };


  public static Number parseQ(String text) throws ParseException {
    return formatQ.get().parse(text);
  }


  public static String formatQ(double x) {
    return formatQ.get().format(x);
  }


  public final static ThreadLocal<SimpleDateFormat> formatDateISO8601
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
      return result;
    }
  };


  public static String formatDateISO8601(Date date) {
    return formatDateISO8601.get().format(date);
  }


  public static Date parseDateISO8601(String text) throws ParseException {
    return formatDateISO8601.get().parse(text);
  }


  public final static ThreadLocal<SimpleDateFormat> formatDateISO8601UTC
          = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat result = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
      result.setTimeZone(TimeZone.getTimeZone("UTC"));
      return result;
    }
  };


  public static String formatDateISO8601UTC(Date date) {
    return formatDateISO8601.get().format(date);
  }


  public static Date parseDateISO8601UTC(String text) throws ParseException {
    return formatDateISO8601.get().parse(text);
  }

}
