/**
 * Copyright 2015-2016 The Splicer Query Engine Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.turn.splicer.tsdbutils.expression;

import com.turn.splicer.tsdbutils.Functions;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class MovingAverageTest {

  @Test
  public void testParseParam() {
    Functions.MovingAverageFunction func = new Functions.MovingAverageFunction();
    long x;
    x = Functions.parseParam("'2min'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES), x);

    x= Functions.parseParam("'1hr'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS), x);

    x= Functions.parseParam("'1000hr'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(1000,TimeUnit.HOURS), x);

    x= Functions.parseParam("'1sec'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(1,TimeUnit.SECONDS), x);

    try {
      Functions.parseParam("'1sechr'");
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      Assert.assertTrue(true);
    }

    try {
      Functions.parseParam("'1 sec'");
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      Assert.assertTrue(true);
    }
  }
}
