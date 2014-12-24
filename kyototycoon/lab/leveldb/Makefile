SHLIBFILES = ktplugdblevel.so

CXX = g++
CPPFLAGS = -I. -I.. -I../.. -I/usr/local/include -D_GNU_SOURCE=1 -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D__EXTENSIONS__
CXXFLAGS = -O2 -Wall -fPIC -fsigned-char
LDFLAGS = -L. -L.. -L../.. -L/usr/local/lib -Wl,-rpath-link,.:..:../..:/usr/local/lib:.:/usr/local/lib:/usr/local/lib:. -Wl,--as-needed
LIBS = -lleveldb -lkyototycoon -lkyotocabinet


all : $(SHLIBFILES)


clean :
	rm -rf $(SHLIBFILES) *.exe *.o a.out check.out gmon.out leak.log casket* *~


.SUFFIXES :
.SUFFIXES : .cc .o

.c.o :
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $<

.cc.o :
	$(CXX) -c $(CPPFLAGS) $(CXXFLAGS) $<


ktplugdblevel.so : ktplugdblevel.o $(LIBRARYFILES)
	$(CXX) $(CXXFLAGS) -shared -o $@ $< $(LDFLAGS) $(LIBS)
