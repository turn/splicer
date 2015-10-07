FROM docker.turn.com/tops-base/jdk7:latest
MAINTAINER instrumentation@turn.com

RUN	      mkdir -p /usr/share/tsdb-splicer/resources/
RUN	      mkdir -p /data/tsdb-splicer/cache

RUN 	  mkdir -p /var/log/tsdb-splicer

RUN 	  ./docker/version.sh

ENV	      CLASSPATH  /usr/share/tsdb-splicer
ENV	      BASE       /usr/share/tsdb-splicer
ENV	      TSDB_PORT  4244

ADD	      tsdb-splicer-all* /usr/share/tsdb-splicer/

ADD	      runner.sh /usr/share/tsdb-splicer/
ADD	      VERSION   /usr/share/tsdb-splicer/
ADD	      resources /usr/share/tsdb-splicer/resources/

WORKDIR   /usr/share/tsdb-splicer/

CMD	      ./runner.sh