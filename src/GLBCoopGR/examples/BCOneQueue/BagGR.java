package GLBCoopGR.examples.BCOneQueue;

import GLBCoopGR.TaskBagGR;
import java.io.Serializable;

public class BagGR implements TaskBagGR, Serializable {

  public int[] data;

  @Override
  public int size() {
    return this.data.length;
  }
}
