package FTGLB;

import java.io.Serializable;

public abstract class FTGLBResult<T extends Serializable> implements Serializable {

  private static final long serialVersionUID = 1L;
  int op;
  private T[] result;

  public FTGLBResult() {
    this.result = null;
    this.op = -1;
  }

  public abstract T[] getResult();

  public void setResult(T[] result) {
    this.result = result;
  }

  public abstract void display(T[] param);

  public T[] submitResult() {
    if (this.result == null) {
      this.result = getResult();
    }
    return this.result;
  }
}
