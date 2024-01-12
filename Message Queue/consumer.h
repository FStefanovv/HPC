#ifndef CONSUMER_H
#define CONSUMER_H

#include <mpi.h>
#include "message_type.h"
#include <stdio.h>
#include "producer.h"
#include <unistd.h>

typedef struct {
    int offset;
} Consumer;

Consumer new_consumer();
void consumer(MPI_Comm topic_comm, int topic);


#endif