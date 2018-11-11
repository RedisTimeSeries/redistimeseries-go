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

RUN go get github.com/stretchr/testify
COPY redis_timeseries redis_timeseries_go
WORKDIR redis_timeseries_go
RUN go get

CMD redis-server --daemonize yes --loadmodule ../redis-timeseries/src/redis-tsdb-module.so && \
    sleep 1 && \
    go test -coverprofile=coverage.out && \
    go tool cover -func=coverage.out
