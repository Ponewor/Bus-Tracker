FROM golang:1.17-rc-buster

#RUN apk add --no-cache build-base

COPY go.mod go.sum main.go ./

RUN unset GOPATH && go build main.go

CMD ./main
