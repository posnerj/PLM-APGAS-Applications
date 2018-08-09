package FTGLB.examples.Pi;

import java.io.Serializable;

public class Pi implements Serializable {

  int N;

  public Pi(int n) {
    this.N = n;
  }

  public static void main(String[] args) {
    int N = 1000000;
    Pi pi = new Pi(N);
    System.out.println("N = " + N);

    double result = pi.calc();

    System.out.println("Result: " + result);
  }

  public double calc() {
    double result = 0;
    double deltaX = 1.0 / N;
    for (int i = 0; i < this.N; i++) {
      result += f(i * deltaX);
    }
    result *= deltaX;
    return result;
  }

  double f(double x) {
    return 4.0 / (1 + x * x);
  }
}
