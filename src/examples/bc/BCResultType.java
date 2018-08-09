package examples.bc;

import java.io.Serializable;

public class BCResultType implements Serializable {

  public final double[] internalResult;

  public BCResultType(int n) {
    this.internalResult = new double[n];
  }
}
