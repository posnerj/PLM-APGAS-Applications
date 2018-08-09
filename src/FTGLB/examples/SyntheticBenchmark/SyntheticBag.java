package FTGLB.examples.SyntheticBenchmark;

import FTGLB.TaskBag;
import GLBCoop.examples.SyntheticBenchmark.SyntheticTask;
import java.io.Serializable;

public class SyntheticBag implements TaskBag, Serializable {

  private static final long serialVersionUID = -6722371507293198586L;

  public SyntheticTask[] tasks;

  public SyntheticBag(int size) {
    tasks = new SyntheticTask[size];
  }

  @Override
  public int size() {
    return tasks.length;
  }
}
