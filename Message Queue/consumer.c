#include "consumer.h"

Consumer new_consumer() {
    Consumer consumer;
    consumer.offset = 0;

    return consumer;
}

void consumer(MPI_Comm topic_comm, int topic) {
    int rank, world_rank, size;
    Message *messages;
    MPI_Status status;

    int received = 0;

    MPI_Comm_rank(topic_comm, &rank);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    int comm_rank;
    MPI_Comm_rank(topic_comm, &comm_rank);
    //printf("rank %d in world, %d in first comm", world_rank, comm_rank);
  
    //simulacija neaktivnosti consumer-a u trenutku kada broker primi i pokusa da posalje poruku
    if((comm_rank == 1 && topic==1) || (comm_rank == 2 && topic==2))
        sleep(10);

    //polling za dati topic, blokirajuce ceka poruke iz topic-a
    while(received<5) {
        MPI_Probe(0, 0, topic_comm, &status);
        MPI_Get_count(&status, message_type, &size);
        messages = (Message*) malloc(size * sizeof(Message));
        MPI_Recv(messages, size, message_type, 0, 0, topic_comm, MPI_STATUS_IGNORE);
        free(messages);
        received+=size;
        if(received==5){
            printf("\nprocess %d done for topic %d (%d in world comm) for the cycle 1\n", comm_rank, topic, world_rank);
            break;
        }
    }
    return;
}

