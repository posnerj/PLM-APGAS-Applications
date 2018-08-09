package IncFTGLB.examples.BackupTester;

import IncFTGLB.IncTaskBag;
import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Bag implements IncTaskBag, Serializable {

  private static final long serialVersionUID = 1L;

  public final Deque<Integer> deque = new LinkedList<>();

  @Override
  public int size() {
    return deque.size();
  }

  @Override
  public void mergeAtTop(IncTaskBag taskBag) {
    Bag bag = (Bag) taskBag;

    Integer[] array = bag.deque.toArray(new Integer[bag.size()]);
    for (int i = 0; i < bag.size(); ++i) {
      deque.addLast(array[i]);
    }
  }
}
