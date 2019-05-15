/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB;

import GLBCoop.TaskBag;
import java.io.Serializable;

public interface FTTaskQueue<Queue, T extends Serializable> extends Serializable {

  boolean process(int n);

  TaskBag split();

  void merge(TaskBag taskBag);

  long count();

  FTGLBResult<T> getResult();

  void printLog();

  /**
   * Merge the result of that into the result of this queue.
   *
   * @param that the other queue.
   */
  void mergeResult(FTTaskQueue<Queue, T> that);

  int size();

  /** Used for fault tolerance deletes all tasks */
  void clearTasks();

  /**
   * Used for fault tolerance
   *
   * @return all tasks
   */
  TaskBag getAllTasks();
}
