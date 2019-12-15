/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.UTS;

import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import utils.SHA1Rand;

/** Created by jposner on 08.11.16. */
public class Bag extends LocalFTTaskBag {
  public SHA1Rand[] hash;
  public int[] lower;
  public int[] upper;

  public Bag(int size) {
    this.hash = new SHA1Rand[size];
    this.lower = new int[size];
    this.upper = new int[size];
  }

  @Override
  public int size() {
    return hash.length;
  }
}
