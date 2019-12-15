/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar;

import static apgas.Constructs.places;

import java.io.Serializable;

public class LocalFTGLBParameters implements Serializable {

  public static final int SHOW_RESULT_FLAG = 1;
  public static final int SHOW_TIMING_FLAG = 2;
  public static final int SHOW_TASKFRAME_LOG_FLAG = 4;
  public static final int SHOW_GLB_FLAG = 8;
  private static final long serialVersionUID = 1L;
  public int n; // at most N task in a batch to run
  public int w; // number of theft attempts before quiescence
  public int l; // power of lifeline graph
  public int z; // base of lifeline graph
  public int m; // max number of thieves
  public int v; // verbose level
  public int timestamps; // >=1 with logging time and n output in cvs, 0=without logging time
  public int P; // count of initial places
  public long s; // Write cyclic backups every s seconds

  public int crashNumber; // number for testing cases

  public int backupCount;

  public LocalFTGLBParameters() {
    this.n = 100;
    this.w = 4;
    this.l = computeL(P);
    this.z = computeZ(l, P);
    this.m = 1024;
    this.v = 15;
    this.timestamps = 0;
    this.crashNumber = 0;
    this.backupCount = 1;
    this.P = places().size();
    this.s = 1;
  }

  public LocalFTGLBParameters(
      int n,
      int w,
      int l,
      int z,
      int m,
      int v,
      int timestamps,
      int crashNumber,
      int backupCount,
      int P,
      long s) {
    this.n = n;
    this.w = w;
    this.l = l;
    this.z = z;
    this.m = m;
    this.v = v;
    this.timestamps = timestamps;
    this.crashNumber = crashNumber;
    this.backupCount = backupCount;
    this.P = P;
    this.s = s;
  }

  public static int computeZ(int l, int numPlaces) {
    int z0 = 1;
    int zz = l;
    while (zz < numPlaces) {
      z0++;
      zz *= l;
    }
    return z0;
  }

  public static int computeL(int numPlaces) {
    int l = 1;
    while (Math.pow(l, l) < numPlaces) {
      l++;
    }
    return l;
  }
}
