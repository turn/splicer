/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
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
    x = func.parseParam("'2min'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES), x);

    x= func.parseParam("'1hr'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS), x);

    x=func.parseParam("'1000hr'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(1000,TimeUnit.HOURS), x);

    x=func.parseParam("'1sec'");
    Assert.assertEquals(TimeUnit.MILLISECONDS.convert(1,TimeUnit.SECONDS), x);

    try {
      func.parseParam("'1sechr'");
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      Assert.assertTrue(true);
    }

    try {
      func.parseParam("'1 sec'");
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      Assert.assertTrue(true);
    }
  }
}
