/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.Prime;

import static apgas.Constructs.here;

import FTGLB.FTGLBResult;
import FTGLB.FTTaskQueue;
import GLBCoop.TaskBag;
import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Queue extends Prime implements FTTaskQueue<Queue, Integer>, Serializable {

  private static final long serialVersionUID = 1L;

  private Deque<Interval> intervals;

  public Queue(int min, int max, int numPlaces) {
    super();
    final int totalTasks = max - min + 1;
    final int tasksPerPlace = (totalTasks + numPlaces - 1) / numPlaces;
    min = here().id * tasksPerPlace;
    max = Math.min(min + tasksPerPlace - 1, max);
    init(min, max);
    intervals = new LinkedList<Interval>();
  }

  @Override
  public boolean process(int n) {
    for (int i = 0; i < n; ++i) {
      while (!intervals.isEmpty() && currentInterval.size() <= 0) {
        currentInterval = intervals.pop();
      }
      next();
    }
    return (size() > 0);
  }

  @Override
  public TaskBag split() {
    if (intervals.size() <= 0 && currentInterval.size() <= 1) {
      return null;
    }

    Bag bag = new Bag();
    if (intervals.size() > 1) {
      for (int i = 0; i < intervals.size() / 2; ++i) {
        Interval offer = intervals.removeLast();
        bag.intervals.add(offer);
        size -= offer.size();
      }
    } else {
      if (currentInterval.size() <= 1) {
        return null;
      }
      Interval offer = currentInterval.split();
      bag.intervals.add(offer);
      size -= offer.size();
    }

    return bag;
  }

  private void merge(Bag bag) {
    if (bag == null || bag.size() <= 0) {
      return;
    }

    for (Interval interval : bag.intervals) {
      intervals.push(interval);
    }
    size += bag.size();
  }

  @Override
  public void merge(TaskBag taskBag) {
    merge((Bag) taskBag);
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public FTGLBResult<Integer> getResult() {
    PrimeResult result = new PrimeResult();
    return result;
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(FTTaskQueue<Queue, Integer> other) {
    count += other.count();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clearTasks() {
    intervals.clear();
    currentInterval = new Interval(0, -1);
    count = 0;
    size = 0;
  }

  @Override
  public TaskBag getAllTasks() {
    Bag bag = new Bag();
    bag.intervals = intervals;
    return bag;
  }

  public class PrimeResult extends FTGLBResult<Integer> implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer[] result;

    public PrimeResult() {
      result = new Integer[1];
    }

    @Override
    public Integer[] getResult() {
      result[0] = count;
      return result;
    }

    @Override
    public void display(Integer[] param) {
      System.out.println("Myresult: " + param[0]);
    }
  }
}
