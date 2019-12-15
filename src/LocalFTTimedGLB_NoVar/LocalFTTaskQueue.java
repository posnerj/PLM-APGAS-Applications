/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar;

import java.io.Serializable;

public interface LocalFTTaskQueue<Queue, T> extends Serializable {
  boolean process(int n);

  LocalFTTaskBag split();

  void merge(LocalFTTaskBag taskBag);

  LocalFTGLBResult<T> getResult();

  void printLog();

  LocalFTTaskBag getAllTasks();

  void clearResult();

  int size();
}
