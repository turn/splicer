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

package com.turn.splicer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Const {

	public static String tsFormat(long val) {
		SimpleDateFormat SDF = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
		return SDF.format(new Date(val));
	}

	/**
	 * Mask to verify a timestamp on 6 bytes in milliseconds
	 */
	public static final long MILLISECOND_MASK = 0xFFFFF00000000000L;

}
