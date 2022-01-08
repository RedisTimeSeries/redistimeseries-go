# This dockerfile is used to run unit tests.

FROM golang:1.17.5

# install redis
RUN git clone -b 5.0 --depth 1 https://github.com/antirez/redis.git
RUN cd redis && make -j install

RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

RUN cd / && git clone https://github.com/RedisLabsModules/redistimeseries.git /redistimeseries
COPY . $GOPATH/src/github.com/RedisLabs/redis-timeseries-go
WORKDIR $GOPATH/src/github.com/RedisLabs/redis-timeseries-go
RUN dep ensure -v

# install redis-timeseries
RUN cd /redistimeseries && \
    git submodule init && \
    git submodule update && \
    git pull --recurse-submodules && \
    cd src && \
    make -j all

CMD redis-server --daemonize yes --loadmodule /redistimeseries/src/redistimeseries.so --requirepass SUPERSECRET && \
    sleep 1 && \
    go test -coverprofile=coverage.out && \
    go tool cover -func=coverage.out
