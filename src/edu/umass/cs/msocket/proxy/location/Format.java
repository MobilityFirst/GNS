/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket.proxy.location;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Implments thread safe versions of formatting and parsing methods using
 * ThreadLocal
 * 
 * @author Westy (westy@cs.umass.edu)
 */
public class Format
{

  public Format()
  {
  }

  /**
   * TIME
   */
  public final static ThreadLocal<DecimalFormat> formatTime = new ThreadLocal<DecimalFormat>()
                                                            {
                                                              @Override
                                                              protected DecimalFormat initialValue()
                                                              {
                                                                return new DecimalFormat("###.##");
                                                              }
                                                            };

  public static Number parseTime(String text) throws ParseException
  {
    return formatTime.get().parse(text);
  }

  /**
   * Decimal time. Format is ###.##"
   * 
   * @param x
   * @return
   */
  public static String formatTime(double x)
  {
    return formatTime.get().format(x);
  }

  /**
   * LATLONG
   */
  public final static ThreadLocal<DecimalFormat> formatLatLong = new ThreadLocal<DecimalFormat>()
                                                               {
                                                                 @Override
                                                                 protected DecimalFormat initialValue()
                                                                 {
                                                                   return new DecimalFormat("####.00000");
                                                                 }
                                                               };

  public static Number parseLatLong(String text) throws ParseException
  {
    return formatLatLong.get().parse(text);
  }

  public static String formatLatLong(double x)
  {
    return formatLatLong.get().format(x);
  }

  /**
   * UTILITY
   */
  public final static ThreadLocal<DecimalFormat> formatUtility = new ThreadLocal<DecimalFormat>()
                                                               {
                                                                 @Override
                                                                 protected DecimalFormat initialValue()
                                                                 {
                                                                   return new DecimalFormat("#.####");
                                                                 }
                                                               };

  public static Number parseUtility(String text) throws ParseException
  {
    return formatUtility.get().parse(text);
  }

  /**
   * Good for values from 0.0 - 1.0, probabilities and such. Format is #.####"
   * 
   * @param x
   * @return
   */
  public static String formatUtility(double x)
  {
    return formatUtility.get().format(x);
  }

  /**
   * dBZ
   */
  public final static ThreadLocal<DecimalFormat> formatdBZ = new ThreadLocal<DecimalFormat>()
                                                           {
                                                             @Override
                                                             protected DecimalFormat initialValue()
                                                             {
                                                               return new DecimalFormat("###.#");
                                                             }
                                                           };

  public static Number parsedBZ(String text) throws ParseException
  {
    return formatdBZ.get().parse(text);
  }

  /**
   * Good for dBZ values. Format is ###.#"
   * 
   * @param x
   * @return
   */
  public static String formatdBZ(double x)
  {
    return formatdBZ.get().format(x);
  }

  /**
   * LONG FLOATs
   */
  public final static ThreadLocal<DecimalFormat> formatFloatLong = new ThreadLocal<DecimalFormat>()
                                                                 {
                                                                   @Override
                                                                   protected DecimalFormat initialValue()
                                                                   {
                                                                     return new DecimalFormat("#####.######");
                                                                   }
                                                                 };

  /**
   * Lots of decimal places. Format is #####.######"
   * 
   * @param x
   * @return
   */
  public static Number parseFloatLong(String text) throws ParseException
  {
    return formatFloatLong.get().parse(text);
  }

  public static String formatFloatLong(double x)
  {
    return formatFloatLong.get().format(x);
  }

  /**
   * FLOATS
   */
  public final static ThreadLocal<DecimalFormat> formatFloat = new ThreadLocal<DecimalFormat>()
                                                             {
                                                               @Override
                                                               protected DecimalFormat initialValue()
                                                               {
                                                                 return new DecimalFormat("####.###");
                                                               }
                                                             };

  public static Number parseFloat(String text) throws ParseException
  {
    return formatFloat.get().parse(text);
  }

  /**
   * Format is "####.###"
   * 
   * @param x
   * @return
   */
  public static String formatFloat(double x)
  {
    return formatFloat.get().format(x);
  }

  public final static ThreadLocal<DecimalFormat> formatFloat2 = new ThreadLocal<DecimalFormat>()
                                                              {
                                                                @Override
                                                                protected DecimalFormat initialValue()
                                                                {
                                                                  return new DecimalFormat("#.0");
                                                                }
                                                              };

  public static Number parseFloat2(String text) throws ParseException
  {
    return formatFloat2.get().parse(text);
  }

  /**
   * Format is "#.0"
   * 
   * @param x
   * @return
   */
  public static String formatFloat2(double x)
  {
    return formatFloat2.get().format(x);
  }

  public final static ThreadLocal<DecimalFormat> formatDistance = new ThreadLocal<DecimalFormat>()
                                                                {
                                                                  @Override
                                                                  protected DecimalFormat initialValue()
                                                                  {
                                                                    return new DecimalFormat("#0.0");
                                                                  }
                                                                };

  public static Number parseDistance(String text) throws ParseException
  {
    return formatDistance.get().parse(text);
  }

  /**
   * Format is #0.0"
   * 
   * @param x
   * @return
   */
  public static String formatDistance(double x)
  {
    return formatDistance.get().format(x);
  }

  /**
   * LONG DATE
   */
  public final static ThreadLocal<SimpleDateFormat> formatPrettyDate = new ThreadLocal<SimpleDateFormat>()
                                                                     {
                                                                       @Override
                                                                       protected SimpleDateFormat initialValue()
                                                                       {
                                                                         return new SimpleDateFormat(
                                                                             "yyyy.MM.dd H:mm:ss zzz");
                                                                       }
                                                                     };

  public static Date parsePrettyDate(String text) throws ParseException
  {
    return formatPrettyDate.get().parse(text);
  }

  /**
   * Format is yyyy.MM.dd H:mm:ss zzz"
   * 
   * @param x
   * @return
   */
  public static String formatPrettyDate(Date date)
  {
    return formatPrettyDate.get().format(date);
  }

  /**
   * LONG DATE IN UTC
   */
  public final static ThreadLocal<SimpleDateFormat> formatPrettyDateUTC = new ThreadLocal<SimpleDateFormat>()
                                                                        {
                                                                          @Override
                                                                          protected SimpleDateFormat initialValue()
                                                                          {
                                                                            SimpleDateFormat result = new SimpleDateFormat(
                                                                                "yyyy.MM.dd H:mm:ss zzz");
                                                                            result.setTimeZone(TimeZone
                                                                                .getTimeZone("UTC"));
                                                                            return result;
                                                                          }
                                                                        };

  /**
   * Format is yyyy.MM.dd H:mm:ss zzz"
   * 
   * @param x
   * @return
   */
  public static String formatPrettyDateUTC(Date date)
  {
    return formatPrettyDateUTC.get().format(date);
  }

  /**
   * TIMESTAMP
   */
  public final static ThreadLocal<SimpleDateFormat> formatDate = new ThreadLocal<SimpleDateFormat>()
                                                               {
                                                                 @Override
                                                                 protected SimpleDateFormat initialValue()
                                                                 {
                                                                   return new SimpleDateFormat("yyyyMMddHHmmss");
                                                                 }
                                                               };

  public static Date parseDate(String text) throws ParseException
  {
    return formatDate.get().parse(text);
  }

  /**
   * Format is yyyyMMddHHmmss"
   * 
   * @param x
   * @return
   */
  public static String formatDate(Date date)
  {
    return formatDate.get().format(date);
  }

  /**
   * TIMESTAMP in UTC
   */
  public final static ThreadLocal<SimpleDateFormat> formatDateUTC = new ThreadLocal<SimpleDateFormat>()
                                                                  {
                                                                    @Override
                                                                    protected SimpleDateFormat initialValue()
                                                                    {
                                                                      SimpleDateFormat result = new SimpleDateFormat(
                                                                          "yyyyMMddHHmmss");
                                                                      result.setTimeZone(TimeZone.getTimeZone("UTC"));
                                                                      return result;
                                                                    }
                                                                  };

  /**
   * Format is yyyyMMddHHmmss"
   * 
   * @param x
   * @return
   */
  public static String formatDateUTC(Date date)
  {
    return formatDateUTC.get().format(date);
  }

  public static Date parseDateUTC(String text) throws ParseException
  {
    return formatDateUTC.get().parse(text);
  }

  public static Date parseDateUTCOrNull(String text)
  {
    try
    {
      return formatDateUTC.get().parse(text);
    }
    catch (Exception e)
    {
      return null;
    }
  }

  /**
   * SHORT DATE
   */
  public final static ThreadLocal<SimpleDateFormat> formatShortDate = new ThreadLocal<SimpleDateFormat>()
                                                                    {
                                                                      @Override
                                                                      protected SimpleDateFormat initialValue()
                                                                      {
                                                                        return new SimpleDateFormat("dd/MM/yy HH:mm:ss");
                                                                      }
                                                                    };

  public static Date parseShortDate(String text) throws ParseException
  {
    return formatShortDate.get().parse(text);
  }

  /**
   * Format is dd/MM/yy HH:mm:ss"
   * 
   * @param x
   * @return
   */
  public static String formatShortDate(Date date)
  {
    return formatShortDate.get().format(date);
  }

  /**
   * SHORT DATE UTC
   */
  public final static ThreadLocal<SimpleDateFormat> formatShortDateUTC = new ThreadLocal<SimpleDateFormat>()
                                                                       {
                                                                         @Override
                                                                         protected SimpleDateFormat initialValue()
                                                                         {
                                                                           SimpleDateFormat result = new SimpleDateFormat(
                                                                               "dd/MM/yy HH:mm:ss");
                                                                           result.setTimeZone(TimeZone
                                                                               .getTimeZone("UTC"));
                                                                           return result;
                                                                         }
                                                                       };

  public static Date parseShortDateUTC(String text) throws ParseException
  {
    return formatShortDateUTC.get().parse(text);
  }

  /**
   * Format is dd/MM/yy HH:mm:ss"
   * 
   * @param x
   * @return
   */
  public static String formatShortDateUTC(Date date)
  {
    return formatShortDateUTC.get().format(date);
  }

  /**
   * TIME ONLY
   */
  public final static ThreadLocal<SimpleDateFormat> formatDateTimeOnly = new ThreadLocal<SimpleDateFormat>()
                                                                       {
                                                                         @Override
                                                                         protected SimpleDateFormat initialValue()
                                                                         {
                                                                           return new SimpleDateFormat("HH:mm:ss zzz");
                                                                         }
                                                                       };

  public static Date parseDateTimeOnly(String text) throws ParseException
  {
    return formatDateTimeOnly.get().parse(text);
  }

  /**
   * Format is HH:mm:ss"
   * 
   * @param x
   * @return
   */
  public static String formatDateTimeOnly(Date date)
  {
    return formatDateTimeOnly.get().format(date);
  }

  //
  public final static ThreadLocal<SimpleDateFormat> formatDateTimeOnlyUTC = new ThreadLocal<SimpleDateFormat>()
                                                                          {
                                                                            @Override
                                                                            protected SimpleDateFormat initialValue()
                                                                            {
                                                                              SimpleDateFormat result = new SimpleDateFormat(
                                                                                  "HH:mm:ss");
                                                                              result.setTimeZone(TimeZone
                                                                                  .getTimeZone("UTC"));
                                                                              return result;
                                                                            }
                                                                          };

  public static Date parseDateTimeOnlyUTC(String text) throws ParseException
  {
    return formatDateTimeOnlyUTC.get().parse(text);
  }

  /**
   * Format is HH:mm:ss"
   * 
   * @param x
   * @return
   */
  public static String formatDateTimeOnlyUTC(Date date)
  {
    return formatDateTimeOnlyUTC.get().format(date);
  }

  /**
   * LONG DATE plus LOCAL TIME
   */
  /**
   * Format is yyyy.MM.dd H:mm:ss zzz [HH:mm:ss]"
   * 
   * @param x
   * @return
   */
  public static String formatDualDate(Date date)
  {
    return formatPrettyDateUTC.get().format(date) + " [" + formatDateTimeOnly.get().format(date) + "]";
  }

  public final static ThreadLocal<SimpleDateFormat> formatDateTimeOnlyMilleUTC = new ThreadLocal<SimpleDateFormat>()
                                                                               {
                                                                                 @Override
                                                                                 protected SimpleDateFormat initialValue()
                                                                                 {
                                                                                   SimpleDateFormat result = new SimpleDateFormat(
                                                                                       "HH:mm:ss.SSS");
                                                                                   result.setTimeZone(TimeZone
                                                                                       .getTimeZone("UTC"));
                                                                                   return result;
                                                                                 }
                                                                               };

  public static Date parseDateTimeOnlyMilleUTC(String text) throws ParseException
  {
    return formatDateTimeOnlyMilleUTC.get().parse(text);
  }

  /**
   * Format is HH:mm:ss.SSS"
   * 
   * @param x
   * @return
   */
  public static String formatDateTimeOnlyMilleUTC(Date date)
  {
    return formatDateTimeOnlyMilleUTC.get().format(date);
  }

  /**
   * LOG FORMAT (UTC)
   */
  public final static ThreadLocal<SimpleDateFormat> formatLogDate = new ThreadLocal<SimpleDateFormat>()
                                                                  {
                                                                    @Override
                                                                    protected SimpleDateFormat initialValue()
                                                                    {
                                                                      SimpleDateFormat result = new SimpleDateFormat(
                                                                          "yyyy-MM-dd'T'HH:mm:ss");
                                                                      result.setTimeZone(TimeZone.getTimeZone("UTC"));
                                                                      return result;
                                                                    }
                                                                  };

  public static Date parseLogDate(String text) throws ParseException
  {
    return formatLogDate.get().parse(text);
  }

  /**
   * Java log file format. Format is yyyy-MM-dd'T'HH:mm:ss"
   * 
   * @param x
   * @return
   */
  public static String formatLogDate(Date date)
  {
    return formatLogDate.get().format(date);
  }

  /**
   * LOG FORMAT (UTC)
   */
  public final static ThreadLocal<SimpleDateFormat> formatMYSQLDate = new ThreadLocal<SimpleDateFormat>()
                                                                    {
                                                                      @Override
                                                                      protected SimpleDateFormat initialValue()
                                                                      {
                                                                        SimpleDateFormat result = new SimpleDateFormat(
                                                                            "yyyy-MM-dd HH:mm:ss");
                                                                        result.setTimeZone(TimeZone.getTimeZone("UTC"));
                                                                        return result;
                                                                      }
                                                                    };

  public static Date parseMYSQLDate(String text) throws ParseException
  {
    return formatMYSQLDate.get().parse(text);
  }

  /**
   * MySQL likes this. Format is yyyy-MM-dd HH:mm:ss"
   * 
   * @param x
   * @return
   */
  public static String formatMYSQLDate(Date date)
  {
    return formatMYSQLDate.get().format(date);
  }

  /**
   * Format For File Names (UTC) - used in renameFileUsingModificationTime
   */
  public final static ThreadLocal<SimpleDateFormat> formatFilenameDate = new ThreadLocal<SimpleDateFormat>()
                                                                       {
                                                                         @Override
                                                                         protected SimpleDateFormat initialValue()
                                                                         {
                                                                           SimpleDateFormat result = new SimpleDateFormat(
                                                                               "yyyy-MM-dd-HH-mm-ss");
                                                                           result.setTimeZone(TimeZone
                                                                               .getTimeZone("UTC"));
                                                                           return result;
                                                                         }
                                                                       };

  public static Date parseFilenameDate(String text) throws ParseException
  {
    return formatFilenameDate.get().parse(text);
  }

  /**
   * Good for writing files with the date and time in them. Format is
   * yyyy-MM-dd-HH-mm-ss"
   * 
   * @param x
   * @return
   */
  public static String formatFilenameDate(Date date)
  {
    return formatFilenameDate.get().format(date);
  }

  public final static ThreadLocal<DecimalFormat> formatQ = new ThreadLocal<DecimalFormat>()
                                                         {
                                                           @Override
                                                           protected DecimalFormat initialValue()
                                                           {
                                                             return new DecimalFormat("0.00");
                                                           }
                                                         };

  public static Number parseQ(String text) throws ParseException
  {
    return formatQ.get().parse(text);
  }

  /**
   * Format is 0.00"
   * 
   * @param x
   * @return
   */
  public static String formatQ(double x)
  {
    return formatQ.get().format(x);
  }
}
