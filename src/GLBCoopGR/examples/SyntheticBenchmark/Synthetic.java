/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoopGR.examples.SyntheticBenchmark;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Random;
import utils.ConsolePrinter;
import utils.MyArrayDeque;

public class Synthetic implements Serializable {

  protected final MyArrayDeque<SyntheticTask> tasks = new MyArrayDeque<>();
  private final transient BigInteger TWO = BigInteger.valueOf(2);
  private final transient BigInteger THREE = BigInteger.valueOf(3);
  private final transient BigInteger FOUR = BigInteger.valueOf(4);
  private final transient BigInteger SEVEN = BigInteger.valueOf(7);
  protected long result = 0;
  protected long count = 0;
  private transient int maxChildren = 0;
  private transient Random randGen = new Random();
  private transient BigInteger q = BigInteger.ONE;
  private transient BigInteger r = BigInteger.ZERO;
  private transient BigInteger t = BigInteger.ONE;
  private transient BigInteger k = BigInteger.ONE;
  private transient BigInteger n = BigInteger.valueOf(3);
  private transient BigInteger l = BigInteger.valueOf(3);

  /*
   * static initialization
   * All tasks are evenly distributed on initialization.
   * No tasks are generated from tasks.
   */
  public Synthetic(int taskBallast, long taskCount, int taskGranularity) {
    this.maxChildren = 0;
    long myTaskCount = taskCount / places().size();
    randGen.setSeed(42 + (here().id * 10));
    for (int i = 0; i < myTaskCount; ++i) {
      tasks.addLast(new SyntheticTask(taskBallast, randGen.nextInt(taskGranularity) + 1));
    }
    if (here().id == 0) {
      long odd = taskCount - (myTaskCount * places().size());
      for (int i = 0; i < odd; i++) {
        tasks.addLast(new SyntheticTask(taskBallast, randGen.nextInt(taskGranularity) + 1));
      }
    }

    if (ConsolePrinter.getInstance().getStatus() == true) {
      ArrayList<Integer> precs = new ArrayList<>();
      int sum = 0;
      for (SyntheticTask t : tasks) {
        precs.add(t.precision);
        sum += t.precision;
      }
      ConsolePrinter.getInstance().println(here() + " sum: " + sum + "....." + precs);
    }
  }

  /*
   *  dynamic initialization
   *  On initialization one task is generated on place 0.
   *  Processing tasks can generate new tasks.
   *
   */
  public Synthetic(int maxTaskChildren) {
    this.maxChildren = maxTaskChildren;
  }

  public void init(int taskBallast, int depth, int taskGranularity) {
    tasks.addLast(new SyntheticTask(taskBallast, 0, depth - 1, taskGranularity));
  }

  public void calculate() {
    SyntheticTask task = this.tasks.pollLast();
    if (task.depth > 0) {
      randGen.setSeed(task.seed);
      int newTaskCount = randGen.nextInt(this.maxChildren) + 1;
      if (newTaskCount > 0) {
        SyntheticTask[] newTasks = new SyntheticTask[newTaskCount];
        for (int i = 0; i < newTaskCount; ++i) {
          newTasks[i] =
              new SyntheticTask(
                  task.ballast.length, task.seed + i + 1, task.depth - 1, task.precision);
        }
        tasks.pushArrayLast(newTasks);
      }
    }

    BigInteger nn, nr;
    for (int i = 0; i < task.precision; ++i) {
      if (FOUR.multiply(q).add(r).subtract(t).compareTo(n.multiply(t)) == -1) {
        nr = BigInteger.TEN.multiply(r.subtract(n.multiply(t)));
        n =
            BigInteger.TEN
                .multiply(THREE.multiply(q).add(r))
                .divide(t)
                .subtract(BigInteger.TEN.multiply(n));
        q = q.multiply(BigInteger.TEN);
        r = nr;
      } else {
        nr = TWO.multiply(q).add(r).multiply(l);
        nn = q.multiply((SEVEN.multiply(k))).add(TWO).add(r.multiply(l)).divide(t.multiply(l));
        q = q.multiply(k);
        t = t.multiply(l);
        l = l.add(TWO);
        k = k.add(BigInteger.ONE);
        n = nn;
        r = nr;
      }
    }

    ++count;
    ++result;
  }
}
