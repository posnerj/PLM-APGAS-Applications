package GLBCoop.examples.SyntheticBenchmark;

import java.io.Serializable;

public class SyntheticTask implements Serializable {

  private static final long serialVersionUID = 2282792464012580417L;

  byte[] ballast;
  long seed;
  int depth;
  int precision;

  public SyntheticTask(int ballastInBytes, int precision) {
    this.ballast = new byte[ballastInBytes];
    this.precision = precision;
  }

  public SyntheticTask(int ballastInBytes, long seed, int depth, int precision) {
    this(ballastInBytes, precision);
    this.seed = seed;
    this.depth = depth;
  }
}
