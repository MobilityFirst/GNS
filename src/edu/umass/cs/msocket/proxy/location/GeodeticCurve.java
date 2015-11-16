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

import java.io.Serializable;

/**
 * This is the outcome of a geodetic calculation. It represents the path and
 * ellipsoidal distance between two GlobalCoordinates for a specified reference
 * ellipsoid.
 */
public class GeodeticCurve implements Serializable
{
  /** Ellipsoidal distance (in meters). */
  private final double mEllipsoidalDistance;

  /** Azimuth (degrees from north). */
  private final double mAzimuth;

  /** Reverse azimuth (degrees from north). */
  private final double mReverseAzimuth;

  /**
   * Create a new GeodeticCurve.
   * 
   * @param ellipsoidalDistance ellipsoidal distance in meters
   * @param azimuth azimuth in degrees
   * @param reverseAzimuth reverse azimuth in degrees
   */
  public GeodeticCurve(double ellipsoidalDistance, double azimuth, double reverseAzimuth)
  {
    mEllipsoidalDistance = ellipsoidalDistance;
    mAzimuth = azimuth;
    mReverseAzimuth = reverseAzimuth;
  }

  /**
   * Get the ellipsoidal distance.
   * 
   * @return ellipsoidal distance in meters
   */
  public double getEllipsoidalDistance()
  {
    return mEllipsoidalDistance;
  }

  /**
   * Get the azimuth.
   * 
   * @return azimuth in degrees
   */
  public double getAzimuth()
  {
    return mAzimuth;
  }

  /**
   * Get the reverse azimuth.
   * 
   * @return reverse azimuth in degrees
   */
  public double getReverseAzimuth()
  {
    return mReverseAzimuth;
  }

  /**
   * Get curve as a string.
   * 
   * @return
   */
  @Override
  public String toString()
  {
    StringBuffer buffer = new StringBuffer();

    buffer.append("s=");
    buffer.append(mEllipsoidalDistance);
    buffer.append(";a12=");
    buffer.append(mAzimuth);
    buffer.append(";a21=");
    buffer.append(mReverseAzimuth);
    buffer.append(";");

    return buffer.toString();
  }
}
