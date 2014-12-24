# Makefile for sample programs of Kyoto Tycoon


#================================================================
# Setting Variables
#================================================================


# Generic settings
SHELL = /bin/sh

# Targets
MYBINS = ktremoteex ktpollex ktthservex kthttpex


# Building binaries
CC = gcc
CXX = g++
CFLAGS = -I. -I.. -Wall -ansi -pedantic -fsigned-char -O2
CXXFLAGS = -I. -I.. -Wall -fsigned-char -O2
LDFLAGS =
LIBS = -L. -L.. -lkyototycoon -lkyotocabinet -lstdc++ -lz -lrt -lpthread -lm -lc
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
	$(RUNENV) ./ktremoteex


.PHONY : all clean static



#================================================================
# Building binaries
#================================================================


ktremoteex : ktremoteex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


ktpollex : ktpollex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


ktthservex : ktthservex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


kthttpex : kthttpex.o
	$(LDENV) $(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)



# END OF FILE
