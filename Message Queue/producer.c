#include "producer.h"

void producer(MPI_Comm producer_comm){

    int sent = 0;

    Message mess;

    while(sent!=10) {
        if(sent==5){
            sleep(5);
        }
        if(sent%2==0) {
            mess.topic = 0;
            strcpy(mess.content, "a message in the first topic...");
        }
        else {
            mess.topic = 1;
            strcpy(mess.content, "a message in the second topic...");
        }
        MPI_Send(&mess, 1, message_type, 0, 0, producer_comm);
        sent+=1;
    }

    // stop signal broker-u za prijem poruka
    mess.topic = -1;
    MPI_Send(&mess, 1, message_type, 0, 0, producer_comm);
}