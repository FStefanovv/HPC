#ifndef MESSAGE_TYPE_H
#define MESSAGE_TYPE_H

#include <mpi.h>

#define MAX_MESSAGE_LEN 96


typedef struct {
    int topic;
    char content[MAX_MESSAGE_LEN];
    int id;
} Message;

extern MPI_Datatype message_type;

void init_message_type();

#endif