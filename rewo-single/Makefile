CC := g++ 
LDLIBS := -lpthread 
CFLAGS := -Wall -g -m64
APPS := rewo

all: $(APPS)

rewo: app.cpp storage.cpp exp.cpp factory.cpp api.cpp hash.cpp siphash.cpp 
	$(CC) -O2 -std=c++11 ${CFLAGS} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o $(APPS)
