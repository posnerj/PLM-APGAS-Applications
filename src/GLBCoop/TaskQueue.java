/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
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
