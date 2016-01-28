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

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.SeekableView;

/**
 * Implementation of SeekableView Interface to wrap around
 * a DataPoint[]
 */
public class SeekableViewDataPointImpl implements SeekableView {

    private DataPoint[] dps;
    private int currentIndex;

    public SeekableViewDataPointImpl(DataPoint[] dps) {

        if(dps == null || dps.length == 0) {
            throw new RuntimeException("Empty or null dps don't know what to do");
        }

        this.dps = dps;
        currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < dps.length;
    }

    @Override
    public DataPoint next() {
        return dps[currentIndex++];
    }

    @Override
    public void remove() {
        //not needed yet?
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void seek(long timestamp) {
        for(int i = currentIndex; i < dps.length; i++) {
            if(dps[i].timestamp() >= timestamp) {
                break;
            } else {
                currentIndex++;

            }
        }
    }
}
