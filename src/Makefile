CC=gcc
CXX=g++
MAKE=make

CFLAGS = -std=c++11 -g -O2 -Wno-sign-compare
CFLAGS += -I "../../protobuf/src"
CFLAGS += -I "../../fmt"
CFLAGS += -I "../../"
CFLAGS += -I "../../cjet/include"


CLIBS =
OBJS = hrpc.pb.o hrpc.o

all: ${OBJS}
	ar rc hrpc.a ${OBJS} ${CLIBS}
	ranlib hrpc.a

hrpc.o:
	${CXX} ${CFLAGS} -o hrpc.o -c hrpc.cc

hrpc.pb.o:
	../../protobuf/build/protoc --cpp_out=. hrpc.proto
	${CXX} ${CFLAGS} -o hrpc.pb.o -c hrpc.pb.cc

clean:
	rm -f hrpc.pb.*
	rm -f ${EXE_SERVER} *.o *.a

