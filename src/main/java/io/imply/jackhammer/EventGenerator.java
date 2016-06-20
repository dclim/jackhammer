/*
 * Copyright 2016 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.imply.jackhammer;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Random;

public class EventGenerator
{
  private static final Random RND = new Random();
  private static final String TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  public static String generate(int numLowCardDims, int numHighCardDims, int lowCardRange, int highCardRange)
  {
    StringBuilder builder = new StringBuilder(150);
    builder.append(
        String.format(
            "{\"timestamp\":\"%s\", \"value\":%d",
            new DateTime(DateTimeZone.UTC).toString(TIME_FORMAT),
            RND.nextInt(1000000)
        )
    );

    for (int i = 0; i < numLowCardDims; i++) {
      builder.append(String.format(", \"dim%d\":\"%s\"", i, RND.nextInt(lowCardRange)));
    }

    for (int i = numLowCardDims; i < numLowCardDims + numHighCardDims; i++) {
      builder.append(String.format(", \"dim%d\":\"%s\"", i, RND.nextInt(highCardRange)));
    }

    return builder.append("}").toString();
  }
}
