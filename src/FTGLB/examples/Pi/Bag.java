package FTGLB.examples.Pi;

import GLBCoop.TaskBag;
import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Bag implements TaskBag, Serializable {

  public Deque<Integer> list = new LinkedList<>();

  @Override
  public int size() {
    return list.size();
  }
}
