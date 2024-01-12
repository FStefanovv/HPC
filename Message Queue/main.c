#include "message_type.h"
#include "topic_queue.h"
#include "consumer.h"
#include "producer.h"
#include "broker.h"

int main(int argc, char **argv){

    int thread_support_mode;
    
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &thread_support_mode);
    
    init_message_type();
    
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    MPI_Group world_group;
    MPI_Comm_group(MPI_COMM_WORLD, &world_group);

    //komunikator u kom su samo producer i broker
    const int producer_group_ids[2] = {0, 1};
    MPI_Group producer_group;
    MPI_Group_incl(world_group, 2, producer_group_ids, &producer_group);
    MPI_Comm producer_comm;
    MPI_Comm_create_group(MPI_COMM_WORLD, producer_group, 0, &producer_comm);

    //komunikator u kom su broker i svi procesi pretplaceni na prvi topic
    const int first_topic_ids[4] = {0, 2, 3, 4};
    MPI_Group first_topic_group;
    MPI_Group_incl(world_group, 4, first_topic_ids, &first_topic_group);
    MPI_Comm first_topic_comm;
    MPI_Comm_create_group(MPI_COMM_WORLD, first_topic_group, 0, &first_topic_comm);

    //komunikator u kom su broker i svi procesi pretplaceni na drugi topic
    const int second_topic_ids[5] = {0, 2, 4, 5, 6};
    MPI_Group second_topic_group;
    MPI_Group_incl(world_group, 5, second_topic_ids, &second_topic_group);
    MPI_Comm second_topic_comm;
    MPI_Comm_create_group(MPI_COMM_WORLD, second_topic_group, 0, &second_topic_comm);

    //pokretanje consumer procesa u zavisnosti od toga da li je pretplacen ili ne za odredjeni topic
    if(world_rank>1) {
        #pragma omp parallel
        {
            #pragma omp single nowait
            {
                if(MPI_COMM_NULL != first_topic_comm)
                    consumer(first_topic_comm, 1);
            }

            #pragma omp single nowait
            {
                if(MPI_COMM_NULL != second_topic_comm)
                    consumer(second_topic_comm, 2);
            }
        }
    }
    //broker proces - prima poruke od producer-a i salje za sve topic-e consumer-ima 
    else if(world_rank==0){
        broker(producer_comm, first_topic_comm, second_topic_comm);
    }
    //producer proces - salje 10 poruka, po 5 svakom topic-u, nakon cega prekida sa radom
    else if(world_rank==1){
        producer(producer_comm);
    }


    //oslobadjanje grupa i komunikatora
    if (MPI_COMM_NULL != first_topic_comm) {
        MPI_Group_free(&first_topic_group);
        MPI_Comm_free(&first_topic_comm);
    }

    if (MPI_COMM_NULL != second_topic_comm) {
        MPI_Group_free(&second_topic_group);
        MPI_Comm_free(&second_topic_comm);
    }

    if (MPI_COMM_NULL != producer_comm){
        MPI_Group_free(&producer_group);
        MPI_Comm_free(&producer_comm);
    }

    MPI_Group_free(&world_group);

    MPI_Finalize();

    return 0;
}

