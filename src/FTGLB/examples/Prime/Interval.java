package FTGLB.examples.Prime;

import java.io.Serializable;

public class Interval implements Serializable {

  private static final long serialVersionUID = 1L;

  public int min;
  public int max;

  public Interval(int min, int max) {
    this.min = min;
    this.max = max;
  }

  public Interval split() {
    Interval splitted = new Interval(min, min + (max - min) / 2);
    min = splitted.max + 1;
    return splitted;
  }

  public int size() {
    return max - min + 1;
  }
}
