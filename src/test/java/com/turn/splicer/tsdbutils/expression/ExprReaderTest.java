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

import org.junit.Assert;
import org.junit.Test;

public class ExprReaderTest {

    @Test(expected = NullPointerException.class)
    public void badInit() {
        new ExprReader(null);
    }

    @Test
    public void initZero() {
        new ExprReader(new char[]{});
    }

    @Test
    public void testPeekNext() {
        ExprReader reader = new ExprReader("hello".toCharArray());

        Assert.assertEquals(reader.peek(), 'h');
        Assert.assertEquals(reader.next(), 'h');

        Assert.assertEquals(reader.peek(), 'e');
        Assert.assertEquals(reader.next(), 'e');

        Assert.assertEquals(reader.peek(), 'l');
        Assert.assertEquals(reader.next(), 'l');

        Assert.assertEquals(reader.peek(), 'l');
        Assert.assertEquals(reader.next(), 'l');

        Assert.assertEquals(reader.peek(), 'o');
        Assert.assertEquals(reader.next(), 'o');
    }

    @Test
    public void testEOF() {
        ExprReader reader = new ExprReader(new char[]{});
        Assert.assertTrue(reader.isEOF());
    }

    @Test
    public void testWhitespace() {
        ExprReader reader = new ExprReader("hello       world".toCharArray());
        for (int i=0; i<5; i++) {
            reader.next();
        }

        Assert.assertFalse(reader.peek() == 'w');
        reader.skipWhitespaces();
        Assert.assertTrue(reader.peek() == 'w');
    }

    @Test
    public void testAttemptWhitespace() {
        ExprReader reader = new ExprReader("hello       world".toCharArray());
        reader.skipWhitespaces();
        Assert.assertTrue(reader.peek() == 'h');
    }

    @Test
    public void testIsNextChar() {
        ExprReader reader = new ExprReader("hello".toCharArray());
        Assert.assertTrue(reader.isNextChar('h'));
        Assert.assertTrue(reader.isNextChar('h'));
        Assert.assertTrue(reader.isNextChar('h'));
    }

    @Test
    public void testIsNextSequence() {
        ExprReader reader = new ExprReader("hello".toCharArray());
        Assert.assertTrue(reader.isNextSeq("he"));
        Assert.assertTrue(reader.isNextSeq("he"));
        Assert.assertTrue(reader.isNextSeq("he"));
    }

}
