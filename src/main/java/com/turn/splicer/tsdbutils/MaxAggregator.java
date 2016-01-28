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

package com.turn.splicer.tsdbutils;

import net.opentsdb.core.Aggregator;

/**
 * Extends the Aggregator interface to provide functionality for "max" style
 * aggregators that first iterate over all data points to determine a max
 */
public interface MaxAggregator extends Aggregator {

    public long[] getLongMaxes();

    public double[] getDoubleMaxes();

    public boolean hasLongs();

    public boolean hasDoubles();
}
