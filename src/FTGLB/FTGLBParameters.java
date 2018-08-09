package FTGLB;

import static apgas.Constructs.places;

import java.io.Serializable;

public class FTGLBParameters implements Serializable {

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
  public int k; // Write cyclic backups every k * n computation-elements
  public int P; // count of initial places

  public int crashNumber; // number for testing cases

  public int backupCount;

  public FTGLBParameters() {
    this.n = 100;
    this.w = 4;
    this.l = 4;
    this.z = computeZ(places().size(), 4);
    this.m = 1024;
    this.v = 15;
    this.timestamps = 0;
    this.crashNumber = 0;
    this.backupCount = 1;
    this.k = 32768;
    this.P = places().size();
  }

  public FTGLBParameters(
      int n,
      int w,
      int l,
      int z,
      int m,
      int v,
      int timestamps,
      int k,
      int crashNumber,
      int backupCount,
      int P) {
    this.n = n;
    this.w = w;
    this.l = l;
    this.z = z;
    this.m = m;
    this.v = v;
    this.timestamps = timestamps;
    this.k = k;
    this.crashNumber = crashNumber;
    this.backupCount = backupCount;
    this.P = P;
  }

  public static int computeZ(long p, int l) {
    int z0 = 1;
    int zz = l;
    while (zz < p) {
      z0++;
      zz *= l;
    }
    return z0;
  }
}
