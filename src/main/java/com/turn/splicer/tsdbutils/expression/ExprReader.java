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

import com.google.common.base.Preconditions;

public class ExprReader {

	protected final char[] chars;

	private int mark = 0;

	public ExprReader(char[] chars) {
		Preconditions.checkNotNull(chars);
		this.chars = chars;
	}

	public int getMark() {
		return mark;
	}

	public char peek() {
		return chars[mark];
	}

	public char next() {
		return chars[mark++];
	}

	public void skip(int num) {
		mark += num;
	}

	public boolean isNextChar(char c) {
		return peek() == c;
	}

	public boolean isNextSeq(CharSequence seq) {
		Preconditions.checkNotNull(seq);
		for (int i = 0; i < seq.length(); i++) {
			if (mark + i == chars.length) return false;
			if (chars[mark + i] != seq.charAt(i)) {
				return false;
			}
		}

		return true;
	}

	public String readFuncName() {
		StringBuilder builder = new StringBuilder();
		while (peek() != '(' && !Character.isWhitespace(peek())) {
			builder.append(next());
		}
		return builder.toString();
	}

	public boolean isEOF() {
		return mark == chars.length;
	}

	public void skipWhitespaces() {
		for (int i = mark; i < chars.length; i++) {
			if (Character.isWhitespace(chars[i])) {
				mark++;
			} else {
				break;
			}
		}
	}

	public String readNextParameter() {
		StringBuilder builder = new StringBuilder();
		int numNested = 0;
		while (!Character.isWhitespace(peek())) {
			char ch = peek();
			if (ch == '(') numNested++;
			else if (ch == ')') numNested--;
			if (numNested < 0) {
				break;
			}
			if (numNested <= 0 && isNextSeq(",,")) {
				break;
			}
			builder.append(next());
		}
		return builder.toString();
	}

	@Override
	public String toString() {
		// make a copy
		return new String(chars);
	}
}
