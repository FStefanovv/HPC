#ifndef BROKER_H
#define BROKER_H

#include <mpi.h>
#include "topic_queue.h"
#include "consumer.h"
#include "producer.h"

void broker(MPI_Comm producer_comm, MPI_Comm first_comm, MPI_Comm second_comm);

#endif