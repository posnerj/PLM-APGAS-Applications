package FTGLB.examples.BCOneQueue;

import FTGLB.TaskBag;
import java.io.Serializable;

public class Bag implements TaskBag, Serializable {

  public int[] data;

  @Override
  public int size() {
    return this.data.length;
  }
}
