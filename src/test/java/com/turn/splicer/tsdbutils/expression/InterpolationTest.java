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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InterpolationTest {

  private static final Logger LOG =
          LoggerFactory.getLogger(AggregationIterator.class);

  @Test
  public void simpleTest() throws Exception {
    int count = 6;
    DataPoint[] points1 = new DataPoint[count];
    points1[0] = MutableDataPoint.ofDoubleValue(0, 5);
    points1[1] = MutableDataPoint.ofDoubleValue(10, 5);
    points1[2] = MutableDataPoint.ofDoubleValue(20, 10);
    points1[3] = MutableDataPoint.ofDoubleValue(30, 15);
    points1[4] = MutableDataPoint.ofDoubleValue(40, 20);
    points1[5] = MutableDataPoint.ofDoubleValue(50, 5);

    DataPoint[] points2 = new DataPoint[count];
    points2[0] = MutableDataPoint.ofDoubleValue(0, 10);
    points2[1] = MutableDataPoint.ofDoubleValue(10, 5);
    points2[2] = MutableDataPoint.ofDoubleValue(20, 20);
    points2[3] = MutableDataPoint.ofDoubleValue(30, 15);
    points2[4] = MutableDataPoint.ofDoubleValue(40, 10);
    points2[5] = MutableDataPoint.ofDoubleValue(50, 0);

    TsdbResult result1 = new TsdbResult();
    result1.setDps(new TsdbResult.Points(new HashMap<String, Object>()));
    for(DataPoint p: points1) {
      result1.getDps().addPoint(p);
    }


    TsdbResult result2 = new TsdbResult();
    result2.setDps(new TsdbResult.Points(new HashMap<String, Object>()));

    for(DataPoint p: points2) {
      result2.getDps().addPoint(p);
    }


    SeekableView[] views = new SeekableView[2];
    views[0] = new SeekableViewDataPointImpl(result1.getDps().getDataPointsFromTreeMap());
    views[1] = new SeekableViewDataPointImpl(result2.getDps().getDataPointsFromTreeMap());



    AggregationIterator aggr = new AggregationIterator(views, 0, 100,
            Aggregators.SUM, Aggregators.Interpolation.LERP, false);

    while (aggr.hasNext()) {
      DataPoint dp = aggr.next();
      LOG.info(dp.timestamp() + " " + (dp.isInteger()? dp.longValue() : dp.doubleValue()));
    }
  }

  @Test
  public void interpolateTest() throws Exception {
    DataPoint[] points1 = new DataPoint[] {
            MutableDataPoint.ofDoubleValue(10, 5),
            MutableDataPoint.ofDoubleValue(30, 15),
            MutableDataPoint.ofDoubleValue(50, 5)
    };

    DataPoint[] points2 = new DataPoint[] {
            MutableDataPoint.ofDoubleValue(0, 10),
            MutableDataPoint.ofDoubleValue(20, 20),
            MutableDataPoint.ofDoubleValue(40, 10),
            MutableDataPoint.ofDoubleValue(60, 20)
    };

    TsdbResult result1 = new TsdbResult();
    result1.setDps(new TsdbResult.Points(new HashMap<String, Object>()));

    for(DataPoint p: points1) {
      result1.getDps().addPoint(p);
    }

    TsdbResult result2 = new TsdbResult();
    result2.setDps(new TsdbResult.Points(new HashMap<String, Object>()));

    for(DataPoint p: points2) {
      result2.getDps().addPoint(p);
    }

    SeekableView[] views = new SeekableView[2];
    views[0] = new SeekableViewDataPointImpl(result1.getDps().getDataPointsFromTreeMap());
    views[1] = new SeekableViewDataPointImpl(result2.getDps().getDataPointsFromTreeMap());

    AggregationIterator aggr = new AggregationIterator(views, 0, 100,
            Aggregators.SUM, Aggregators.Interpolation.LERP, false);

    while (aggr.hasNext()) {
      DataPoint dp = aggr.next();
      LOG.info(dp.timestamp() + " " + (dp.isInteger()? dp.longValue() : dp.doubleValue()));
    }
  }

  static class MockDataPoints implements DataPoints {

    @Override
    public String metricName() {
      return "testMetric";
    }

    @Override
    public Deferred<String> metricNameAsync() {
      return Deferred.fromResult(metricName());
    }

    @Override
    public Map<String, String> getTags() {
      Map<String, String> p = Maps.newHashMap();
      p.put("tagk", "tagv");
      return p;
    }

    @Override
    public Deferred<Map<String, String>> getTagsAsync() {
      return Deferred.fromResult(getTags());
    }

    @Override
    public List<String> getAggregatedTags() {
      return Lists.newArrayList("type");
    }

    @Override
    public Deferred<List<String>> getAggregatedTagsAsync() {
      return Deferred.fromResult(getAggregatedTags());
    }

    @Override
    public List<String> getTSUIDs() {
      return null;
    }

    @Override
    public int size() {
      throw new RuntimeException("not implemented");
    }

    @Override
    public int aggregatedSize() {
      throw new RuntimeException("not implemented");
    }

    @Override
    public SeekableView iterator() {
      throw new RuntimeException("not implemented");
    }

    @Override
    public long timestamp(int i) {
      throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isInteger(int i) {
      throw new RuntimeException("not implemented");
    }

    @Override
    public long longValue(int i) {
      throw new RuntimeException("not implemented");
    }

    @Override
    public double doubleValue(int i) {
      throw new RuntimeException("not implemented");
    }
  }

}
