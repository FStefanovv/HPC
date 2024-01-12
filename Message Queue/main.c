#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include <mpi.h>
#include <omp.h>
#include <pthread.h>

#include "message_type.h"
#include "topic_queue.h"
#include "consumer.h"



void producer(MPI_Comm producer_comm);
void broker(MPI_Comm producer_comm, MPI_Comm first_comm);
void consumer(MPI_Comm first_topic_comm);

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

    //consumer procesi za prvi topic
    if(MPI_COMM_NULL != first_topic_comm && world_rank > 1){
        consumer(first_topic_comm);
    }
    //broker proces - prima producer i komunikatore za sve topic-e 
    else if(world_rank==0){
        broker(producer_comm, first_topic_comm);
    }
    //producer proces
    else if(world_rank==1){
        producer(producer_comm);
    }

    if (MPI_COMM_NULL != first_topic_comm) {
        MPI_Group_free(&first_topic_group);
        MPI_Comm_free(&first_topic_comm);
    }

    if (MPI_COMM_NULL != producer_comm){
        MPI_Group_free(&producer_group);
        MPI_Comm_free(&producer_comm);
    }

    MPI_Group_free(&world_group);

    MPI_Finalize();

    return 0;
}

void broker(MPI_Comm producer_comm, MPI_Comm first_comm){

    TopicQueue* first_topic = new_queue("first");
    TopicQueue* second_topic = new_queue("second");

    //dobavljanje broja consumer-a za prvi topic
    int first_topic_consumer_num;
    MPI_Comm_size(first_comm, &first_topic_consumer_num);
    first_topic_consumer_num--;

    //postavljanje offset-a za svakog consumer-a na 0 - metoda new_consumer()
    Consumer first_topic_consumers[first_topic_consumer_num];
    for(int i=0; i< first_topic_consumer_num; i++) {
        first_topic_consumers[i] = new_consumer();
    }

    #pragma omp parallel num_threads(1+first_topic_consumer_num) 
    {
        //nit za prijem poruka od producer-a
        #pragma omp single nowait
        {
            Message current;
            //printf("\nreceiving getting executed by thread %d", omp_get_thread_num());

            while(1) {
                MPI_Recv(&current, 1, message_type, 1, 0, producer_comm, MPI_STATUS_IGNORE);
                if(current.topic == -1){
                    break;
                }
                if(current.topic==0) {
                    enqueue(current, first_topic);
                }
                else {
                    enqueue(current, second_topic);
                }
            }
        }

        //nit koja pravi task-ove za svakog consumer-a u okviru prvog topic-a
        #pragma omp single
        {
            int id;
            MPI_Comm_rank(first_comm, &id);
            //printf("I have the rank %d in first topic comm and i am the broker", id);
            for(int i=0; i<first_topic_consumer_num; i++) 
            {  
                #pragma omp task 
                {
                    int current = i;
                    //printf("\nsending sequentially, current consumer is %d", current);
                    //printf("\niteration %d of for loop getting executed by %d", current, omp_get_thread_num());
                    while(1) {
                        int current_offset = first_topic_consumers[current].offset;
                        if(current_offset < first_topic->current){
                            int number_of_messages;
                            Message *to_send = readMessages(current_offset, &number_of_messages, *first_topic);
                            printf("\nhave %d messages to send to process %d, current offset %d", number_of_messages, current+1, current_offset);
                            MPI_Send(to_send, number_of_messages, message_type, current+1, 0,  first_comm);   
                            first_topic_consumers[current].offset+=number_of_messages;
                            printf("\nsent %d messages to process %d, current_offset %d", number_of_messages, current+1, first_topic_consumers[current].offset);   
                            free(to_send);
                            
                        }     
                        
                        if(first_topic_consumers[current].offset == 5){
                            //printf("\nconsumer %d done", current+1);
                            break; 
                        }
                        
                        
                        //printf("\nafter second if block");
                    }  
                }               
            }
        }
    }
}

void consumer(MPI_Comm topic_comm) {
    int rank, world_rank, size;
    Message *messages;
    MPI_Status status;

    int received = 0;

    MPI_Comm_rank(topic_comm, &rank);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    int id;
    MPI_Comm_rank(topic_comm, &id);
    //printf("rank %d in world, %d in first comm", world_rank, id);


    //polling za dati topic, blokirajuce ceka poruke iz topic-a
    while(received<5) {

        MPI_Probe(0, 0, topic_comm, &status);
        MPI_Get_count(&status, message_type, &size);
        messages = (Message*) malloc(size * sizeof(Message));
        MPI_Recv(messages, size, message_type, 0, 0, topic_comm, MPI_STATUS_IGNORE);
        free(messages);
        received+=size;
        //printf("\nreceived %d messages", received);

        if(received==5){
            printf("\nreceived %d messages, breaking", received);
            break;
        }
    }
    return;
}


void producer(MPI_Comm producer_comm){

    int sent = 0;

    Message mess;

    while(sent!=10) {
        if(sent==5)
            sleep(5);
        if(sent%2==0) {
            mess.topic = 0;
            strcpy(mess.content, "some random message in the first topic...");
        }
        else {
            mess.topic = 1;
            strcpy(mess.content, "a message in the second topic...");
        }
        MPI_Send(&mess, 1, message_type, 0, 0, producer_comm);
        sent+=1;
    }

    //printf("\nI am done sending");
    // stop signal broker-u za prijem poruka
    mess.topic = -1;
    MPI_Send(&mess, 1, message_type, 0, 0, producer_comm);
}
