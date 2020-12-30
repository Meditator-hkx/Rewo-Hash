CC := g++ 
LDLIBS := -lpthread 
CFLAGS := -Wall -g -m64 -mrtm
APPS := rewo

all: $(APPS)

rewo: main.cpp exp.cpp cli.cpp api.cpp storage.cpp factory.cpp hash.cpp 
	$(CC) -std=c++11 ${CFLAGS} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o $(APPS)
