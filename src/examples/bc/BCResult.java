package examples.bc;

import apgas.impl.ResultAsyncAny;
import java.util.Arrays;

/** Created by jposner on 15.03.17. */
public class BCResult extends ResultAsyncAny<BCResultType> {

  public BCResult(int N) {
    this.result = new BCResultType(N);
    Arrays.fill(this.result.internalResult, 0.0);
  }

  private static String sub(String str, int start, int end) {
    return (str.substring(start, Math.min(end, str.length())));
  }

  @Override
  public void mergeResult(ResultAsyncAny<BCResultType> result) {
    if (result == null) {
      return;
    }

    BCResultType rr = result.getResult();
    for (int i = 0; i < this.result.internalResult.length; i++) {
      this.result.internalResult[i] += rr.internalResult[i];
    }
  }

  @Override
  public void mergeResult(BCResultType result) {
    for (int i = 0; i < this.result.internalResult.length; i++) {
      this.result.internalResult[i] += result.internalResult[i];
    }
  }

  @Override
  public void display() {
    for (int i = this.result.internalResult.length - 1; i > 0; i--) {
      if (0.0 != this.result.internalResult[i]) {
        System.out.println("(" + i + ") -> " + sub("" + this.result.internalResult[i], 0, 6));
        return;
      }
    }
  }

  @Override
  public String toString() {
    return Double.toString(this.result.internalResult[0]);
  }

  @Override
  public BCResult clone() {
    BCResult bcResult = new BCResult(this.result.internalResult.length);
    System.arraycopy(
        this.result.internalResult,
        0,
        bcResult.result.internalResult,
        0,
        this.result.internalResult.length);
    return bcResult;
  }
}
