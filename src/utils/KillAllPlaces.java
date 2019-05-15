/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package utils;

import static apgas.Constructs.here;
import static apgas.Constructs.immediateAsyncAt;
import static apgas.Constructs.places;

import apgas.GlobalRuntime;
import apgas.Place;

public class KillAllPlaces {

  public static void doit() {
    for (Place place : places()) {
      if (place.id == here().id) {
        continue;
      }
      try {
        immediateAsyncAt(
            place,
            () -> {
              GlobalRuntime.getRuntime().setPlaceFailureHandler((deadPlace) -> System.exit(404));
              System.exit(404);
            });
      } catch (Throwable throwable) {
      }
    }
    System.exit(404);
  }
}
