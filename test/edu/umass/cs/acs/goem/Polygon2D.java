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
package edu.umass.cs.acs.goem;

import edu.umass.cs.acs.geodesy.GlobalCoordinate;
import java.util.ArrayList;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.Line2D;
import java.awt.geom.PathIterator;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.geom.Area;
import java.awt.geom.GeneralPath;
import java.awt.Graphics2D;
import java.util.Collection;
import java.util.List;

/**
 * A polygon shape. This class implements <b>java.awt.Shape</b>, and
 * consists of a series of straight-line segments. This class
 * should be used instead of GeneralPath for shapes that consist
 * only of straight-line segments and are always closed. It is
 * more efficient than GeneralPath, and allows the coordinates of
 * vertices to be modified.
 *
 * Following the convention set by the Java2D shape classes,
 * the Polygon class is an abstract class, which contains
 * two concrete inner classes, one storing floats and one
 * storing doubles.
 *
 */
public abstract class Polygon2D implements Shape, Cloneable {

  /**
   * The current number of coordinates
   */
  protected int _coordCount = 0;

  /**
   * The flag that says the the polygon has been closed
   */
  protected boolean _closed = false;

  @Override
  public abstract Object clone() throws CloneNotSupportedException;

  /**
   * Close the polygon. No further segments can be added.
   * If this method not called, then the path iterators will
   * treat the polygon as thought it were closed, and implicitly
   * join the most recently added vertex to the first one. However,
   * this method should generally be called, as if the last vertex
   * is the same as the first vertex, then it merges them.
   */
  public void closePathDeletingLastVertexSameAsFirst() {
    if ((getX(getVertexCount() - 1) == getX(0))
            && (getY(getVertexCount() - 1) == getY(0))) {
      _coordCount -= 2;
    }

    _closed = true;
  }

  /**
   * Return true if the given point is inside the polygon.
   * This method uses a straight-forward algorithm, where a point
   * is taken to be inside the polygon if a horizontal line
   * extended from the point to infinity intersects an odd number
   * of segments.
   */
  @Override
  public boolean contains(double x, double y) {
    int crossings = 0;

    if (_coordCount == 0) {
      return false;
    }

    // Iterate over all vertices
    int i = 1;

    for (; i < getVertexCount();) {
      double x1 = getX(i - 1);
      double x2 = getX(i);
      double y1 = getY(i - 1);
      double y2 = getY(i);

      // Crossing if lines intersect
      if ((x < x1) || (x < x2)) {
        if (y == y2) {
          crossings++;
        } else if (y == y1) {
          // do nothing, so that two adjacent segments
          // don't both get counted
        } else if (Line2D.linesIntersect(x, y, Math.max(x1, x2), y, x1,
                y1, x2, y2)) {
          crossings++;
        }
      }

      i++;
    }

    // Final segment
    double x1 = getX(i - 1);
    double y1 = getY(i - 1);
    double x2 = getX(0);
    double y2 = getY(0);

    // Crossing if lines intersect
    if ((x < x1) || (x < x2)) {
      if (Line2D.linesIntersect(x, y, Math.max(x1, x2), y, x1, y1, x2, y2)
              && (y != y1)) {
        crossings++;
      }
    }

    // True if odd number of crossings
    return (crossings % 2) == 1;
  }

  /**
   * Return true if the given point is inside the polygon.
   */
  @Override
  public boolean contains(Point2D p) {
    return contains(p.getX(), p.getY());
  }

//    public boolean contains(LatLong p) {
//        return contains(p.getLong(), p.getLat());
//    }
  /**
   * Return true if the given rectangle is entirely inside
   * the polygon. (Currently, this algorithm can be fooled by
   * supplying a rectangle that has all four corners inside the
   * polygon, but which intersects some edges.)
   */
  @Override
  public boolean contains(Rectangle2D r) {
    return contains(r.getX(), r.getY(), r.getWidth(), r.getHeight());
  }

  /**
   * Return true if the given rectangle is entirely inside
   * the polygon. (Currently, this algorithm can be fooled by
   * supplying a rectangle that has all four corners inside the
   * polygon, but which intersects some edges.)
   * @param x1
   * @param y1
   */
  @Override
  public boolean contains(double x1, double y1, double w, double h) {
    double x2 = x1 + w;
    double y2 = y1 + h;
    return contains(x1, y1) && contains(x1, y2) && contains(x2, y1)
            && contains(x2, y2);
  }

  /**
   * Assuming both polygons are convex return true if the given
   * polygon is entirely inside this polygon.
   *
   * @param poly
   * @return
   */
  public boolean containsConvex(Polygon2D poly) {
    for (int i = 0; i < getVertexCount(); i++) {
      if (!contains(getX(i), getY(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the integer bounds of the polygon.
   */
  @Override
  public Rectangle getBounds() {
    return getBounds2D().getBounds();
  }

  /**
   * Get the floating-point bounds of the polygon.
   */
  @Override
  public abstract Rectangle2D getBounds2D();

  /**
   * Get a path iterator over the object.
   */
  @Override
  public PathIterator getPathIterator(AffineTransform at, double flatness) {
    return getPathIterator(at);
  }

  /**
   * Get a path iterator over the object.
   */
  @Override
  public PathIterator getPathIterator(AffineTransform at) {
    return new PolygonIterator(this, at);
  }

  /**
   * Get the number of vertices
   *
   * @return
   */
  public int getVertexCount() {
    return _coordCount / 2;
  }

  /**
   * Get the given X-coordinate
   *
   * @param index
   * @return 
   * @exception IndexOutOfBoundsException The index is out of bounds.
   */
  public abstract double getX(int index);

  /**
   * Get the given Y-coordinate
   *
   * @param index
   * @return 
   * @exception IndexOutOfBoundsException The index is out of bounds.
   */
  public abstract double getY(int index);

  /**
   * Test if the polygon is intersected by the given
   * rectangle. (Currently, this algorithm can be fooled by
   * supplying a rectangle that has no corners inside the
   * polygon, and does not contain any vertex of the polygon,
   * but which intersects some edges.)
   */
  @Override
  public boolean intersects(Rectangle2D r) {
    return intersects(r.getX(), r.getY(), r.getWidth(), r.getHeight());
  }

  /**
   * Test if the polygon is intersected by the given
   * rectangle.
   * @param x1
   * @param y1
   */
  @Override
  public boolean intersects(double x1, double y1, double w, double h) {
    double x2 = x1 + w;
    double y2 = y1 + h;

    if (_coordCount == 0) {
      return false;
    }

    // If the bounds don't intersect, then return false.
    Rectangle2D rect = new Rectangle2D.Double(x1, y1, w, h);

    if (!getBounds().intersects(rect)) {
      return false;
    }

    // return true if the polygon contains any vertex of the rectangle.
    if (contains(x1, y1) || contains(x1, y2) || contains(x2, y1)
            || contains(x2, y2)) {
      return true;
    }

    // return true if the rectangle contains any vertex of the polygon
    for (int i = 0; i < getVertexCount(); i++) {
      if (rect.contains(getX(i), getY(i))) {
        return true;
      }
    }

    // return true if any line segment of the polygon crosses a line
    // segment of the rectangle.
    // This is rather long, I wonder if it could be optimized.
    // Iterate over all vertices
    for (int i = 1; i < getVertexCount(); i++) {
      double vx1 = getX(i - 1);
      double vx2 = getX(i);
      double vy1 = getY(i - 1);
      double vy2 = getY(i);

      if (Line2D.linesIntersect(x1, y1, x1, y2, vx1, vy1, vx2, vy2)) {
        return true;
      }

      if (Line2D.linesIntersect(x1, y2, x2, y2, vx1, vy1, vx2, vy2)) {
        return true;
      }

      if (Line2D.linesIntersect(x2, y2, x2, y1, vx1, vy1, vx2, vy2)) {
        return true;
      }

      if (Line2D.linesIntersect(x2, y1, x1, y1, vx1, vy1, vx2, vy2)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Add a new vertex to the end of the polygon.
   * Throw an exception of the polygon has already been closed.
   * @param x
   * @param y
   */
  public abstract void lineTo(double x, double y);

  /**
   * Move the start point of the vertex to the given position.
   * Throw an exception if the line already contains any vertices.
   * @param x
   * @param y
   */
  public abstract void moveTo(double x, double y);

  /**
   * Reset the polygon back to empty.
   */
  public void reset() {
    _coordCount = 0;
    _closed = false;
  }

  /**
   * Set the given X-coordinate.
   *
   * @param index
   * @param x
   * @exception IndexOutOfBoundsException The index is out of bounds.
   */
  public abstract void setX(int index, double x);

  /**
   * Set the given Y-coordinate
   *
   * @param index
   * @param y
   * @exception IndexOutOfBoundsException The index is out of bounds.
   */
  public abstract void setY(int index, double y);

  /**
   * Transform the polygon with the given transform.
   * @param at
   */
  public abstract void transform(AffineTransform at);

  /**
   * Translate the polygon the given distance.
   *
   * @param x
   * @param y
   */
  public abstract void translate(double x, double y);

  /**
   * Return a string representation of the polygon.
   */
  @Override
  public String toString() {
    String out = getClass().getName() + "[\n";

    for (int i = 0; i < getVertexCount(); i++) {
      out = out + "\t" + getX(i) + ", " + getY(i) + "\n";
    }

    out = out + "]";
    return out;
  }

  /**
   * Function to calculate the area of a polygon, according to the algorithm
   * defined at http://local.wasp.uwa.edu.au/~pbourke/geometry/polyarea/
   *
   * @return area of the polygon defined by pgPoints
   */
  public double area() {
    int i, j;
    int n = getVertexCount();
    double area = 0;

    for (i = 0; i < n; i++) {
      j = (i + 1) % n;
      area += getX(i) * getY(j);
      area -= getX(j) * getY(i);
    }
    area /= 2.0;
    return (area < 0 ? -area : area); // for unsigned
    // return (area); for signed
  }

  /**
   * Function to calculate the center of mass for a given polygon, according
   * ot the algorithm defined at
   * http://local.wasp.uwa.edu.au/~pbourke/geometry/polyarea/
   *
   * @return point that is the center of mass
   */
  public Point2D centerOfMass() {
    double cx = 0, cy = 0;
    double area = area();
    Point2D res = new Point2D.Double();
    // int i, j, n = polyPoints.length;
    int i, j, n = getVertexCount();

    double factor = 0;
    for (i = 0; i < n; i++) {
      j = (i + 1) % n;
      // factor = (polyPoints[i].getX() * polyPoints[j].getY() - polyPoints[j].getX() * polyPoints[i].getY());
      // cx += (polyPoints[i].getX() + polyPoints[j].getX()) * factor;
      // cy += (polyPoints[i].getY() + polyPoints[j].getY()) * factor;

      factor = (getX(i) * getY(j) - getX(j) * getY(i));
      cx += (getX(i) + getX(j)) * factor;
      cy += (getY(i) + getY(j)) * factor;
    }
    area *= 6.0f;
    factor = 1 / area;
    cx *= factor;
    cy *= factor;
    res.setLocation(cx, cy);
    return res;
  }

  /**
   * Subtracts the polygon from this polygon and returns the polygon that is left.
   */
  public Polygon2D notIntersection(Polygon2D poly) {
    Area polyArea = new Area(poly);
    Area thisArea = new Area(this);
    //System.out.println("isEmpty " + thisArea.isEmpty());	
    //System.out.println("isPolygonal " + thisArea.isPolygonal());	
    thisArea.subtract(polyArea);
    //System.out.println("after isEmpty " + thisArea.isEmpty());	
    //System.out.println("after isPolygonal " + thisArea.isPolygonal());	
    double[] coordinates = getPath(thisArea.getPathIterator(null));
    return new Double(coordinates);
  }

  /**
   * Subtracts the polygon from this polygon and returns the percentage of
   * this polygon that is left.
   *
   * @param poly
   * @return
   */
  public double percentNotIntersection(Polygon2D poly) {
    double it = notIntersection(poly).area();
    double res = it / this.area();
    //System.out.println(" " + it + " / " + this.area() + " = " + res);
    return res;
  }

  public double[] getPath(PathIterator pi) {
    ArrayList points = new ArrayList();
    while (pi.isDone() == false) {
      double[] coordinates = new double[6];
      int type = pi.currentSegment(coordinates);
      switch (type) {
        case PathIterator.SEG_MOVETO:
          //System.out.println("move to " + coordinates[0] + ", " + coordinates[1]);
          points.add(new java.lang.Double(coordinates[0]));
          points.add(new java.lang.Double(coordinates[1]));
          break;
        case PathIterator.SEG_LINETO:
          //System.out.println("line to " + coordinates[0] + ", " + coordinates[1]);
          points.add(new java.lang.Double(coordinates[0]));
          points.add(new java.lang.Double(coordinates[1]));
          break;
        case PathIterator.SEG_QUADTO:
          //System.out.println("quadratic to " + coordinates[0] + ", " + coordinates[1] + ", "
          //	   + coordinates[2] + ", " + coordinates[3]);
          break;
        case PathIterator.SEG_CUBICTO:
          //System.out.println("cubic to " + coordinates[0] + ", " + coordinates[1] + ", "
          //	   + coordinates[2] + ", " + coordinates[3] + ", " + coordinates[4] + ", " + coordinates[5]);
          break;
        case PathIterator.SEG_CLOSE:
          //System.out.println("close");
          break;
        default:
          break;
      }
      pi.next();
    }
    double result[] = new double[points.size()];
    int i = 0;
    for (Object d : points) {
      java.lang.Double e = (java.lang.Double) d;
      result[i++] = e.doubleValue();
    }
    return result;
  }

  public void draw(Graphics2D g2) {
    GeneralPath polygon = new GeneralPath(GeneralPath.WIND_EVEN_ODD, _coordCount);
    polygon.moveTo((float) getX(0), (float) getY(0));
    for (int i = 0; i < getVertexCount(); i++) {
      polygon.lineTo((float) getX(i), (float) getY(i));
    }
    polygon.closePath();
    g2.draw(polygon);
  }

  // FLOAT IMPLEMENTATION
  /**
   * The concrete Polygon class that stores coordinates internally
   * as floats.
   */
  public static class Float extends Polygon2D implements Cloneable {

    /**
     * The coordinates
     */
    float[] _coords;

    /**
     * Create a new polygon with space for the
     * given number of vertices.
     *
     * @param size
     */
    public Float(int size) {
      _coords = new float[2 * size];
    }

    /**
     * Create a new polygon with the
     * given vertices, in the format
     * [x0, y0, x1, y1, ... ].
     *
     * @param coords
     */
    public Float(float[] coords) {
      _coords = coords;
      _coordCount = coords.length;
    }

    /**
     * Create a new polygon with no vertices.
     */
    public Float() {
      this(2);
    }

    /**
     * Create a new polygon with a single start point
     *
     * @param x
     * @param y
     */
    public Float(float x, float y) {
      this(2);
      _coords[0] = x;
      _coords[1] = y;
      _coordCount = 2;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      Polygon2D.Float cloned = (Polygon2D.Float) new Float(_coordCount);
      cloned._coordCount = _coordCount;
      cloned._closed = _closed;
      System.arraycopy(_coords, 0, cloned._coords, 0, _coordCount);
      return cloned;
    }

    /**
     * Get the floating-point bounds of the polygon.
     * @return a rectangle
     */
    @Override
    public Rectangle2D getBounds2D() {
      if (_coordCount <= 1) {
        return new Rectangle2D.Float();
      }

      float x1 = _coords[0];
      float y1 = _coords[1];
      float x2 = x1;
      float y2 = y1;

      for (int i = 2; i < _coordCount;) {
        if (_coords[i] < x1) {
          x1 = _coords[i];
        } else if (_coords[i] > x2) {
          x2 = _coords[i];
        }

        i++;

        if (_coords[i] < y1) {
          y1 = _coords[i];
        } else if (_coords[i] > y2) {
          y2 = _coords[i];
        }

        i++;
      }

      return new Rectangle2D.Float(x1, y1, x2 - x1, y2 - y1);
    }

    /**
     * Get the given X-coordinate
     *
     * @return the x coordinate
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public double getX(int index) {
      return _coords[index * 2];
    }

    /**
     * Get the given Y-coordinate
     *
     * @return the y coordinate
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public double getY(int index) {
      return _coords[(index * 2) + 1];
    }

    /**
     * Add a new vertex to the end of the line.
     */
    @Override
    public void lineTo(double x, double y) {
      if (_closed) {
        throw new UnsupportedOperationException(
                "This polygon has already been closed");
      }

      if (_coordCount == _coords.length) {
        float[] temp = new float[_coordCount * 2];
        System.arraycopy(_coords, 0, temp, 0, _coordCount);
        _coords = temp;
      }

      _coords[_coordCount++] = (float) x;
      _coords[_coordCount++] = (float) y;
    }

    /**
     * Move the start point of the vertex to the given position.
     *
     * @exception UnsupportedOperationException The polygon already
     * has vertices
     */
    @Override
    public void moveTo(double x, double y) {
      if (_coordCount > 0) {
        throw new UnsupportedOperationException(
                "This polygon already has vertices");
      }

      _coords[0] = (float) x;
      _coords[1] = (float) y;
      _coordCount = 2;
    }

    /**
     * Set the given X-coordinate.
     *
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public void setX(int index, double x) {
      _coords[index * 2] = (float) x;
    }

    /**
     * Set the given Y-coordinate
     *
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public void setY(int index, double y) {
      _coords[(index * 2) + 1] = (float) y;
    }

    /**
     * Transform the polygon with the given transform.
     */
    @Override
    public void transform(AffineTransform at) {
      at.transform(_coords, 0, _coords, 0, _coordCount / 2);
    }

    /**
     * Translate the polygon the given distance.
     */
    @Override
    public void translate(double x, double y) {
      float fx = (float) x;
      float fy = (float) y;

      for (int i = 0; i < _coordCount;) {
        _coords[i++] += fx;
        _coords[i++] += fy;
      }
    }
  }

  // DOUBLE IMPLEMENTATION
  /**
   * The concrete Polygon class that stores coordinates internally
   * as doubles.
   */
  public static class Double extends Polygon2D implements Cloneable {

    /**
     * The coordinates
     */
    double[] _coords;

    @Override
    public Object clone() throws CloneNotSupportedException {
      Polygon2D.Double cloned = (Polygon2D.Double) new Double(_coordCount);
      cloned._coordCount = _coordCount;
      cloned._closed = _closed;
      System.arraycopy(_coords, 0, cloned._coords, 0, _coordCount);
      return cloned;
    }

    /**
     * Create a new polygon with space for the
     * given number of vertices.
     *
     * @param size
     */
    public Double(int size) {
      _coords = new double[2 * size];
    }

    /**
     * Create a new polygon with the
     * given vertices, in the format
     * [x0, y0, x1, y1, ... ].
     *
     * @param coords
     */
    public Double(double[] coords) {
      _coords = coords;
      _coordCount = coords.length;
    }

    /**
     * Create a new polygon with no coordinates
     */
    public Double() {
      this(2);
    }

    /**
     * Create a new polygon with a single start point
     *
     * @param x
     * @param y
     */
    public Double(double x, double y) {
      this(2);
      _coords[0] = x;
      _coords[1] = y;
      _coordCount = 2;
    }

    /**
     * Create a new polygon from a collection of Points.
     * 
     * @param points 
     */
    public Double(Collection<PointSimple> points) {
      this(points.size());
      for (PointSimple item : points) {
        lineTo(item.getX(), item.getY());
      }
    }
    
     public Double(List<GlobalCoordinate> points) {
      this(points.size());
      for (GlobalCoordinate coord : points) {
        lineTo(coord.getLong(), coord.getLat());
      }  
    }
     
    /**
     * Get the floating-point bounds of the polygon.
     * @return a rectangle
     */
    @Override
    public Rectangle2D getBounds2D() {
      if (_coordCount <= 0) {
        return new Rectangle2D.Double();
      }

      double x1 = _coords[0];
      double y1 = _coords[1];
      double x2 = x1;
      double y2 = y1;

      for (int i = 2; i < _coordCount;) {
        if (_coords[i] < x1) {
          x1 = _coords[i];
        } else if (_coords[i] > x2) {
          x2 = _coords[i];
        }

        i++;

        if (_coords[i] < y1) {
          y1 = _coords[i];
        } else if (_coords[i] > y2) {
          y2 = _coords[i];
        }

        i++;
      }

      return new Rectangle2D.Double(x1, y1, x2 - x1, y2 - y1);
    }

    /**
     * Get the number of vertices
     * @return the number of vertices
     */
    @Override
    public int getVertexCount() {
      return _coordCount / 2;
    }

    /**
     * Get the given X-coordinate
     *
     * @return 
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public double getX(int index) {
      return _coords[index * 2];
    }

    /**
     * Get the given Y-coordinate
     *
     * @return 
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public double getY(int index) {
      return _coords[(index * 2) + 1];
    }

    /**
     * Add a new vertex to the end of the line.
     */
    @Override
    public void lineTo(double x, double y) {
      if (_closed) {
        throw new UnsupportedOperationException(
                "This polygon has already been closed");
      }

      if (_coordCount == _coords.length) {
        double[] temp = new double[_coordCount * 2];
        System.arraycopy(_coords, 0, temp, 0, _coordCount);
        _coords = temp;
      }

      _coords[_coordCount++] = x;
      _coords[_coordCount++] = y;
    }

    /**
     * Move the start point of the vertex to the given position.
     *
     * @exception UnsupportedOperationException The polygon already
     * has vertices
     */
    @Override
    public void moveTo(double x, double y) {
      if (_coordCount > 0) {
        throw new UnsupportedOperationException(
                "This polygon already has vertices");
      }

      _coords[0] = x;
      _coords[1] = y;
      _coordCount = 2;
    }

    /**
     * Set the given X-coordinate.
     *
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public void setX(int index, double x) {
      _coords[index * 2] = x;
    }

    /**
     * Set the given Y-coordinate
     *
     * @exception IndexOutOfBoundsException The index is out of bounds.
     */
    @Override
    public void setY(int index, double y) {
      _coords[(index * 2) + 1] = y;
    }

    /**
     * Transform the polygon with the given transform.
     */
    @Override
    public void transform(AffineTransform at) {
      at.transform(_coords, 0, _coords, 0, _coordCount / 2);
    }

    /**
     * Translate the polygon the given distance.
     */
    @Override
    public void translate(double x, double y) {
      for (int i = 0; i < _coordCount;) {
        _coords[i++] += x;
        _coords[i++] += y;
      }
    }
  }
}
