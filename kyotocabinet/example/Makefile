# Makefile for sample programs of Kyoto Cabinet



#================================================================
# Setting Variables
#================================================================


# Generic settings
SHELL = /bin/sh

# Targets
MYBINS = kcprotoex kccacheex kcgrassex kchashex kctreeex kcdirex kcforestex \
  kcpolyex kcvisex kcmrex kclangcex

# Building binaries
CC = gcc
CXX = g++
CFLAGS = -I. -I.. -Wall -ansi -pedantic -fsigned-char -O2
CXXFLAGS = -I. -I.. -Wall -fsigned-char -O2
LDFLAGS =
LIBS = -L. -L.. -lkyotocabinet -lstdc++ -lz -lrt -lpthread -lm -lc
LDENV = LD_RUN_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..
RUNENV = LD_LIBRARY_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..



#================================================================
# Suffix rules
#================================================================


.SUFFIXES :
.SUFFIXES : .c .cc .o

.c.o :
	$(CC) -c $(CFLAGS) $<

.cc.o :
	$(CXX) -c $(CXXFLAGS) $<



#================================================================
# Actions
#================================================================


all : $(MYBINS)


clean :
	rm -rf $(MYBINS) *.exe *.o a.out check.out gmon.out leak.log casket* *~


static :
	make LDFLAGS="$(LDFLAGS) -static"


check :
	rm -rf casket*
	$(RUNENV) ./kcprotoex
	$(RUNENV) ./kccacheex
	$(RUNENV) ./kcgrassex
	$(RUNENV) ./kchashex
	$(RUNENV) ./kctreeex
	$(RUNENV) ./kcdirex
	$(RUNENV) ./kcforestex
	$(RUNENV) ./kcpolyex
	$(RUNENV) ./kcvisex
	$(RUNENV) ./kcmrex
	$(RUNENV) ./kclangcex


.PHONY : all clean static



#================================================================
# Building binaries
#================================================================


kcprotoex : kcprotoex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kccacheex : kccacheex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kcgrassex : kcgrassex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kchashex : kchashex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kctreeex : kctreeex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kcdirex : kcdirex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kcforestex : kcforestex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kcpolyex : kcpolyex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kcvisex : kcvisex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kcmrex : kcmrex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kclangcex : kclangcex.o
	$(LDENV) $(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)



# END OF FILE
