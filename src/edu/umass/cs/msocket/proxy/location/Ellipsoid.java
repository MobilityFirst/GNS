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
 * Encapsulation of an ellipsoid, and declaration of common reference
 * ellipsoids.
 */
public class Ellipsoid implements Serializable
{
  /** Semi major axis (meters). */
  private final double mSemiMajorAxis;

  /** Semi minor axis (meters). */
  private final double mSemiMinorAxis;

  /** Flattening. */
  private final double mFlattening;

  /** Inverse flattening. */
  private final double mInverseFlattening;

  /**
   * Construct a new Ellipsoid. This is private to ensure the values are
   * consistent (flattening = 1.0 / inverseFlattening). Use the methods
   * fromAAndInverseF() and fromAAndF() to create new instances.
   * 
   * @param semiMajor
   * @param semiMinor
   * @param flattening
   * @param inverseFlattening
   */
  private Ellipsoid(double semiMajor, double semiMinor, double flattening, double inverseFlattening)
  {
    mSemiMajorAxis = semiMajor;
    mSemiMinorAxis = semiMinor;
    mFlattening = flattening;
    mInverseFlattening = inverseFlattening;
  }

  /** The WGS84 ellipsoid. */
  static public final Ellipsoid WGS84      = fromAAndInverseF(6378137.0, 298.257223563);

  /** The GRS80 ellipsoid. */
  static public final Ellipsoid GRS80      = fromAAndInverseF(6378137.0, 298.257222101);

  /** The GRS67 ellipsoid. */
  static public final Ellipsoid GRS67      = fromAAndInverseF(6378160.0, 298.25);

  /** The ANS ellipsoid. */
  static public final Ellipsoid ANS        = fromAAndInverseF(6378160.0, 298.25);

  /** The WGS72 ellipsoid. */
  static public final Ellipsoid WGS72      = fromAAndInverseF(6378135.0, 298.26);

  /** The Clarke1858 ellipsoid. */
  static public final Ellipsoid Clarke1858 = fromAAndInverseF(6378293.645, 294.26);

  /** The Clarke1880 ellipsoid. */
  static public final Ellipsoid Clarke1880 = fromAAndInverseF(6378249.145, 293.465);

  /** A spherical "ellipsoid". */
  static public final Ellipsoid Sphere     = fromAAndF(6371000, 0.0);

  /**
   * Build an Ellipsoid from the semi major axis measurement and the inverse
   * flattening.
   * 
   * @param semiMajor semi major axis (meters)
   * @param inverseFlattening
   * @return
   */
  static public Ellipsoid fromAAndInverseF(double semiMajor, double inverseFlattening)
  {
    double f = 1.0 / inverseFlattening;
    double b = (1.0 - f) * semiMajor;

    return new Ellipsoid(semiMajor, b, f, inverseFlattening);
  }

  /**
   * Build an Ellipsoid from the semi major axis measurement and the flattening.
   * 
   * @param semiMajor semi major axis (meters)
   * @param flattening
   * @return
   */
  static public Ellipsoid fromAAndF(double semiMajor, double flattening)
  {
    double inverseF = 1.0 / flattening;
    double b = (1.0 - flattening) * semiMajor;

    return new Ellipsoid(semiMajor, b, flattening, inverseF);
  }

  /**
   * Get semi-major axis.
   * 
   * @return semi-major axis (in meters).
   */
  public double getSemiMajorAxis()
  {
    return mSemiMajorAxis;
  }

  /**
   * Get semi-minor axis.
   * 
   * @return semi-minor axis (in meters).
   */
  public double getSemiMinorAxis()
  {
    return mSemiMinorAxis;
  }

  /**
   * Get flattening
   * 
   * @return
   */
  public double getFlattening()
  {
    return mFlattening;
  }

  /**
   * Get inverse flattening.
   * 
   * @return
   */
  public double getInverseFlattening()
  {
    return mInverseFlattening;
  }
}
