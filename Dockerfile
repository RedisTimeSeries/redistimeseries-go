# This dockerfile is used to run unit tests.

FROM golang:1.11.1

# install redis
RUN git clone -b 5.0 --depth 1 https://github.com/antirez/redis.git
RUN cd redis && make -j install

# install redis-timeseries
RUN git clone https://github.com/RedisLabsModules/redis-timeseries.git
RUN cd redis-timeseries && \
    git submodule init && \
    git submodule update && \
    cd src && \
    make -j all

RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

WORKDIR /go/src/github.com/RedisLabs/redis-timeseries-go
COPY * ./
RUN dep ensure

CMD redis-server --daemonize yes --loadmodule /go/redis-timeseries/src/redis-tsdb-module.so --requirepass SUPERSECRET && \
    sleep 1 && \
    go test -coverprofile=coverage.out && \
    go tool cover -func=coverage.out
