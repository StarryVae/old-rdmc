all: rdmc-mcast

rdmc-mcast: rdmc.cpp rdmc.h message.h psm_helper.h util.cpp util.h experiment.cpp microbenchmarks.h microbenchmarks.cpp group_send.cpp group_send.h Makefile
	g++ -std=c++14 rdmc.cpp util.cpp experiment.cpp microbenchmarks.cpp group_send.cpp -o psm-mcast -lpsm_infinipath -ggdb -O3 -lboost_system -lslurm -Wall -fno-omit-frame-pointer
