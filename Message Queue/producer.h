#ifndef PRODUCER_H
#define PORDUCER_H

#include <mpi.h>
#include "message_type.h"
#include <string.h>
#include <stdio.h>
#include "stdlib.h"
#include <unistd.h>


void producer(MPI_Comm producer_comm);

#endif