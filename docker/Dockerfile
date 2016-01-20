FROM docker.turn.com/tops-base/jdk7:latest
MAINTAINER instrumentation@turn.com

ENV       VERSION 0.1.5-2016011901
ENV       WORKDIR /usr/share/tsdb-splicer
ENV       DATADIR /data/tsdb-splicer
ENV       LOGDIR  /var/log/tsdb-splicer
RUN	      mkdir -p $WORKDIR/resources
RUN	      mkdir -p $DATADIR/cache
RUN       mkdir -p $LOGDIR

ENV	      CLASSPATH  $WORKDIR
ENV	      BASE       $WORKDIR
ENV	      SPLICER_PORT  4245
EXPOSE    $SPLICER_PORT

ADD	      tsdb-splicer-all-$VERSION.jar $WORKDIR/

ADD	      VERSION   $WORKDIR/resources/
ADD	      resources $WORKDIR/resources/

WORKDIR   $WORKDIR

ENTRYPOINT /bin/bash -c "java -server -classpath $WORKDIR/resources:$WORKDIR:$WORKDIR/tsdb-splicer-all-$VERSION.jar com.turn.splicer.SplicerMain --port $SPLICER_PORT --config=$WORKDIR/resources/splicer.conf"
