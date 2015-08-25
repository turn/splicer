package com.turn.splicer.tsdbutils.expression;

import com.turn.splicer.tsdbutils.DataPoint;
import com.turn.splicer.tsdbutils.SeekableView;

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
        return currentIndex < dps.length - 1;
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
