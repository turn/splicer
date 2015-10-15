FROM docker.turn.com/tops-base/jdk7:latest
MAINTAINER instrumentation@turn.com

ENV       WORKDIR /usr/share/tsdb-splicer
ENV       DATADIR /data/tsdb-splicer
ENV       LOGDIR  /var/log/tsdb-splicer
RUN	      mkdir -p $WORKDIR/resources
RUN	      mkdir -p $DATADIR/cache

RUN 	    mkdir -p $LOGDIR

ENV	      CLASSPATH  $WORKDIR
ENV	      BASE       $WORKDIR
ENV	      SPLICER_PORT  4245
EXPOSE    $SPLICER_PORT

ADD	      tsdb-splicer-all* $WORKDIR/

ADD	      VERSION   $WORKDIR/resources/
ADD	      resources $WORKDIR/resources/

WORKDIR   $WORKDIR

CMD	      java -server -classpath $WORKDIR/resources:$WORKDIR/tsdb-splicer-all-0.1.2-14oct15.jar com.turn.splicer.SplicerMain --port $SPLICER_PORT --config=$WORKDIR/resources/splicer.conf
