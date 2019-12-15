/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.SyntheticBenchmark;

import GLBCoop.examples.SyntheticBenchmark.SyntheticTask;
import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import java.io.Serializable;

public class SyntheticBag extends LocalFTTaskBag implements Serializable {
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
