package GLBCoop;

import java.io.Serializable;

public interface TaskQueue<Queue, T extends Serializable> extends Serializable {

  boolean process(int n);

  TaskBag split();

  void merge(TaskBag taskBag);

  long count();

  GLBResult<T> getResult();

  void printLog();

  /**
   * Merge the result of that into the result of this queue.
   *
   * @param that the other queue.
   */
  void mergeResult(TaskQueue<Queue, T> that);

  int size();
}
