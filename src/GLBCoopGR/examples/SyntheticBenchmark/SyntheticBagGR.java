package GLBCoopGR.examples.SyntheticBenchmark;

import GLBCoopGR.TaskBagGR;
import java.io.Serializable;

public class SyntheticBagGR implements TaskBagGR, Serializable {

  private static final long serialVersionUID = -6722371507293198586L;

  public SyntheticTask[] tasks;

  public SyntheticBagGR(int size) {
    tasks = new SyntheticTask[size];
  }

  @Override
  public int size() {
    return tasks.length;
  }
}
