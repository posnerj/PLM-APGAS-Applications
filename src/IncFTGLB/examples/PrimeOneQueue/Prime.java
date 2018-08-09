package IncFTGLB.examples.PrimeOneQueue;

import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

// Calculates the prime-counting function using trial division.
public class Prime implements Serializable {

  private static final long serialVersionUID = 1L;

  protected Deque<Integer> deque = new LinkedList<Integer>();
  protected int count;

  public static void main(String[] args) {
    Prime prime = new Prime();
    prime.init(0, 1_000_000);

    long time = System.currentTimeMillis();
    while (prime.next()) {;
    }
    time = System.currentTimeMillis() - time;

    System.out.println("took " + time + " msecs with count = " + prime.count);
  }

  protected void init(int min, int max) {
    for (int i = min; i <= max; ++i) {
      deque.addLast(i);
    }
  }

  protected boolean next() {
    if (deque.isEmpty()) {
      return false;
    }

    final int n = deque.pollLast();

    boolean prime = true;
    for (int i = 2; i < n; ++i) {
      if (n % i == 0) {
        prime = false;
        break;
      }
    }

    if (prime && n > 1) {
      ++count;
    }

    return true;
  }
}
