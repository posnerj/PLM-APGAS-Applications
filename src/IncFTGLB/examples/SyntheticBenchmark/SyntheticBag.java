package IncFTGLB.examples.SyntheticBenchmark;

import GLBCoop.examples.SyntheticBenchmark.SyntheticTask;
import IncFTGLB.IncTaskBag;
import java.io.Serializable;

public class SyntheticBag implements IncTaskBag, Serializable {

  private static final long serialVersionUID = -6722371507293198586L;

  public SyntheticTask[] tasks;

  public SyntheticBag(int size) {
    tasks = new SyntheticTask[size];
  }

  @Override
  public int size() {
    return tasks.length;
  }

  @Override
  public void mergeAtTop(IncTaskBag bag) {
    SyntheticBag taskBag = (SyntheticBag) bag;
    int newSize = tasks.length + taskBag.tasks.length;
    SyntheticTask[] newTasks = new SyntheticTask[newSize];
    System.arraycopy(tasks, 0, newTasks, 0, tasks.length);
    System.arraycopy(taskBag.tasks, 0, newTasks, tasks.length, taskBag.tasks.length);

    this.tasks = newTasks;
  }
}
