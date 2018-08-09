package utils;

import java.io.Serializable;

public class Pair<T extends Serializable, U extends Serializable> implements Serializable {

  private T first;
  private U second;

  public Pair(T first, U second) {
    this.first = first;
    this.second = second;
  }

  public T getFirst() {
    return first;
  }

  public void setFirst(T first) {
    this.first = first;
  }

  public U getSecond() {
    return second;
  }

  public void setSecond(U second) {
    this.second = second;
  }
}
