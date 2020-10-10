CC := g++ 
LDLIBS := -lpthread 
CFLAGS := -Wall -g -m64
APPS := rewo

all: $(APPS)

rewo: main.cpp exp.cpp cli.cpp api.cpp storage.cpp factory.cpp hash.cpp 
	$(CC) -O2 -std=c++11 ${CFLAGS} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o $(APPS)
