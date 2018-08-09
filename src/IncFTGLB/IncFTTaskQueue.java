package IncFTGLB;

import java.io.Serializable;

public interface IncFTTaskQueue<Queue, T extends Serializable> extends Serializable {

  void process();

  long count();

  void setCount(long count);

  IncFTGLBResult<T> getResult();

  /**
   * Used for incremental backups Sets the result.
   *
   * @param result to set.
   */
  void setResult(IncFTGLBResult<T> result);

  void printLog();

  IncTaskBag split();

  /**
   * Merge the result of that into the result of this queue.
   *
   * @param that the other queue.
   */
  void mergeResult(IncFTTaskQueue<Queue, T> that);

  int size();

  /**
   * Used for incremental backups
   *
   * @param that the other TaskBag.
   */
  void mergeAtBottom(IncTaskBag that);

  /**
   * Used for incremental backups
   *
   * @param that the other TaskBag.
   */
  void mergeAtTop(IncTaskBag that);

  /**
   * Used for incremental backups
   *
   * @param n Count of tasks to be removed from the bottom.
   * @return TaskBag containing only the removed tasks
   */
  IncTaskBag removeFromBottom(long n);

  /**
   * Used for incremental backups
   *
   * @param n Count of tasks to be removed from the top.
   * @return TaskBag containing only the removed tasks
   */
  IncTaskBag removeFromTop(long n);

  /**
   * Used for incremental backups
   *
   * @return TaskBag containing only the next task to be processed.
   */
  IncTaskBag getTopElement();

  /**
   * Used for incremental backups
   *
   * @return TaskBag containing n tasks from the bottom skipping offset tasks.
   */
  IncTaskBag getFromBottom(long n, long offset);
}
