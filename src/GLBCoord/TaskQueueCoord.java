/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoord;

import GLBCoop.GLBResult;
import GLBCoop.TaskBag;
import java.io.Serializable;

public interface TaskQueueCoord<Queue, T extends Serializable> extends Serializable {

  boolean process(int n);

  TaskBag split(); // has to be synchronized

  void mergePublic(TaskBag taskBag);

  long count();

  GLBResult<T> getResult();

  void printLog();

  /**
   * Merge the result of that into the result of this queue.
   *
   * @param that the other queue.
   */
  void mergeResult(TaskQueueCoord<Queue, T> that);

  int size();

  void release();

  int getCountRelease();

  int getCountRequire();

  int privateSize();

  int publicSize();

  int getElementsSize();
}
