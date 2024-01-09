#include "message_type.h"

MPI_Datatype message_type;


void init_message_type() {

    int lengths[3] = {1, 1, MAX_MESSAGE_LEN};
    MPI_Datatype types[] = {MPI_INT, MPI_INT, MPI_CHAR};

    MPI_Aint displacements[3];
    MPI_Aint base_address;

    Message message;
    
    MPI_Get_address(&message, &base_address);

    MPI_Get_address(&message.topic, &displacements[0]);
    MPI_Get_address(&message.id, &displacements[1]);
    MPI_Get_address(&message.content, &displacements[2]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);
    displacements[2] = MPI_Aint_diff(displacements[2], base_address);


    MPI_Type_create_struct(3, lengths, displacements, types, &message_type);
    MPI_Type_commit(&message_type);
}