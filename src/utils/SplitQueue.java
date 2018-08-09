package utils;

import java.io.Serializable;
import java.lang.reflect.Array;

/** Created by jonas on 31.08.2015. */
public class SplitQueue<E> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final int MIN_INITIAL_CAPACITY = 2048;
  private static double maxSplitPercent = 0.5;
  // for testing
  public int countRelease = 0;
  public int countReacquire = 0;
  protected volatile int tail;
  protected volatile int head;
  protected volatile int split; // first element from private area
  transient E[] elements; // non-private to simplify nested class access
  private Class<E> c;

  @SuppressWarnings("unchecked")
  public SplitQueue(Class<E> c) {
    this.elements = (E[]) Array.newInstance(c, MIN_INITIAL_CAPACITY);
    this.c = c;
  }

  @SuppressWarnings({"unchecked", "AccessStaticViaInstance"})
  public SplitQueue(double maxSplit, Class<E> c) {
    this.elements = (E[]) Array.newInstance(c, MIN_INITIAL_CAPACITY);
    this.c = c;
    this.maxSplitPercent = maxSplit;
  }

  public SplitQueue(int numElements, Class<E> c) {
    this.c = c;
    allocateElements(numElements);
  }

  public SplitQueue(int numElements, Class<E> c, double split) {
    SplitQueue.maxSplitPercent = split;
    this.c = c;
    allocateElements(numElements);
  }

  public int getElementsSize() {
    return this.elements.length;
  }

  @SuppressWarnings("unchecked")
  private void allocateElements(int numElements) {
    int initialCapacity = MIN_INITIAL_CAPACITY;
    // Find the best power of two to hold elements.
    // Tests "<=" because arrays aren't kept full.
    if (numElements >= initialCapacity) {
      initialCapacity = numElements;
      initialCapacity |= (initialCapacity >>> 1);
      initialCapacity |= (initialCapacity >>> 2);
      initialCapacity |= (initialCapacity >>> 4);
      initialCapacity |= (initialCapacity >>> 8);
      initialCapacity |= (initialCapacity >>> 16);
      initialCapacity++;

      if (initialCapacity < 0) // Too many elements, must back off
      {
        initialCapacity >>>= 1; // Good luck allocating 2 ^ 30 elements
      }
    }
    this.elements = (E[]) Array.newInstance(c, initialCapacity);
  }

  @SuppressWarnings("unchecked")
  private synchronized void doubleCapacity() {
    assert tail == head;
    int p = tail;
    int n = elements.length;
    int r = n - p; // number of elements to the right of p
    int newCapacity = n << 1;
    if (newCapacity < 0) {
      throw new IllegalStateException("Sorry, deque too big");
    }
    E[] a = (E[]) Array.newInstance(c, newCapacity);
    System.arraycopy(elements, p, a, 0, r);
    System.arraycopy(elements, 0, a, r, p);
    elements = a;
    tail = 0;
    head = n;
    this.split = (split - p) & (n - 1);
  }

  public int size() {
    return (head - tail) & (elements.length - 1);
  }

  public int privateSize() {
    return (head - split) & (elements.length - 1);
  }

  public int publicSize() {
    return (split - tail) & (elements.length - 1);
  }

  public synchronized boolean isEmpty() {
    return tail == head;
  }

  public synchronized void reacquire() {
    if (privateSize() < publicSize()) {
      calculateSplit();
      this.countReacquire++;
    }
  }

  public void release() {
    if (privateSize() > 0) {
      calculateSplit();
      this.countRelease++;
    }
  }

  public void pushPrivate(E e) {
    if (e == null) {
      throw new NullPointerException();
    }

    elements[head] = e;

    synchronized (this) {
      if ((head = (head + 1) & (elements.length - 1)) == tail) {
        doubleCapacity();
      }
    }
  }

  private synchronized void pushPublic(E e) {
    if (e == null) {
      throw new NullPointerException();
    }

    tail = (tail - 1) & (elements.length - 1);

    elements[tail] = e;

    if (head == tail) {
      doubleCapacity();
    }
  }

  private void calculateSplit() {
    double d = tail + size() * maxSplitPercent;
    this.split = (int) (d) & (elements.length - 1);
  }

  public void pushPrivate(E[] array) {
    for (E e : array) {
      pushPrivate(e);
    }
  }

  public synchronized void pushPublic(E[] array) {
    for (E e : array) {
      pushPublic(e);
    }

    reacquire();
  }

  public synchronized E popPublic() {
    if (publicSize() <= 0) {
      return null;
    }

    E result = this.elements[tail];
    this.tail = (this.tail + 1) & (this.elements.length - 1);

    return result;
  }

  @SuppressWarnings("unchecked")
  public synchronized E[] popPublic(int n) {
    int toCopy = Math.min(n, this.publicSize());
    if (toCopy <= 0) {
      return null;
    }

    E[] result = (E[]) Array.newInstance(c, toCopy);
    int firstCopy = Math.min(toCopy, this.elements.length - this.tail);

    System.arraycopy(this.elements, this.tail, result, 0, firstCopy);
    if (firstCopy < toCopy) {
      int secondCopy = toCopy - firstCopy;
      System.arraycopy(this.elements, 0, result, firstCopy, secondCopy);
    }

    this.tail = (this.tail + toCopy) & (this.elements.length - 1);

    return result;
  }

  public E popPrivate() {

    if (privateSize() <= 0) {
      return null;
    }

    E result = this.elements[(this.head - 1) & (this.elements.length - 1)];
    this.head = (this.head - 1) & (this.elements.length - 1);

    reacquire();

    return result;
  }

  public synchronized void clear() {
    this.head = 0;
    this.tail = 0;
    this.split = 0;
  }
}
