package examples.pi;

import apgas.impl.ResultAsyncAny;

/** Created by jposner on 15.03.17. */
public class PiResult extends ResultAsyncAny<Double> {

  public PiResult(double r) {
    this.result = r;
  }

  @Override
  public void mergeResult(ResultAsyncAny<Double> r) {
    this.result += r.getResult();
  }

  @Override
  public void mergeResult(Double result) {
    this.result += result;
  }

  @Override
  public void display() {
    System.out.println("Pi result: " + this.result);
  }

  @Override
  public String toString() {
    return Double.toString(this.result);
  }

  @Override
  public PiResult clone() {
    return new PiResult(this.result);
  }
}
