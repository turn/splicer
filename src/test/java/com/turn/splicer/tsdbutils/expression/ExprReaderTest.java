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
