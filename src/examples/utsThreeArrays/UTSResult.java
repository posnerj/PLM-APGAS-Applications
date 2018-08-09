package examples.utsThreeArrays;

import apgas.impl.ResultAsyncAny;

/** Created by jposner on 15.03.17. */
public class UTSResult extends ResultAsyncAny<Long> {

  public UTSResult() {
    this.result = 0L;
  }

  public UTSResult(Long r) {
    this.result = r;
  }

  @Override
  public void mergeResult(ResultAsyncAny<Long> r) {
    if (r == null) {
      return;
    }
    this.result += r.getResult();
  }

  @Override
  public void mergeResult(Long result) {
    this.result += result;
  }

  @Override
  public void display() {
    System.out.println("StartUTS result: " + this.result);
  }

  @Override
  public String toString() {
    return Long.toString(this.result);
  }

  @Override
  public UTSResult clone() {
    return new UTSResult(this.result);
  }
}
