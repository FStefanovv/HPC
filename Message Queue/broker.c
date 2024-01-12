#include "broker.h"

void broker(MPI_Comm producer_comm, MPI_Comm first_comm, MPI_Comm second_comm){

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

    int second_topic_consumer_num;
    MPI_Comm_size(second_comm, &second_topic_consumer_num);
    second_topic_consumer_num--;

    //postavljanje offset-a za svakog consumer-a na 0 - metoda new_consumer()
    Consumer second_topic_consumers[second_topic_consumer_num];
    for(int i=0; i<second_topic_consumer_num; i++) {
        second_topic_consumers[i] = new_consumer();
    }

    #pragma omp parallel num_threads(1+first_topic_consumer_num+second_topic_consumer_num) 
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
                    //printf("\nI am the task for consumer %d and my pid is %d", current, getpid());
                    //printf("\nsending sequentially, current consumer is %d", current);
                    //printf("\niteration %d of for loop getting executed by %d", current, omp_get_thread_num());
                    while(1) {
                        int current_offset = first_topic_consumers[current].offset;
                        if(current_offset < first_topic->current){
                            int number_of_messages;
                            Message *to_send = readMessages(current_offset, &number_of_messages, *first_topic);
                            //printf("\nhave %d messages to send to process %d, current offset %d", number_of_messages, current+1, current_offset);
                            MPI_Send(to_send, number_of_messages, message_type, current+1, 0,  first_comm);
                            first_topic_consumers[current].offset+=number_of_messages;
                            printf("\nsent %d messages to process %d, current_offset %d (first topic)", number_of_messages, current+1, first_topic_consumers[current].offset);   
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


        #pragma omp single
        {
            int id;
            MPI_Comm_rank(second_comm, &id);
            //printf("I have the rank %d in first topic comm and i am the broker", id);
            for(int i=0; i<second_topic_consumer_num; i++) 
            {  
                #pragma omp task 
                {
                    int current = i;
                    //printf("\nI am the task for consumer %d and my pid is %d", current, getpid());
                    //printf("\nsending sequentially, current consumer is %d", current);
                    //printf("\niteration %d of for loop getting executed by %d", current, omp_get_thread_num());
                    while(1) {
                        int current_offset = second_topic_consumers[current].offset;
                        if(current_offset < second_topic->current){
                            int number_of_messages;
                            Message *to_send = readMessages(current_offset, &number_of_messages, *second_topic);
                            //printf("\nhave %d messages to send to process %d, current offset %d", number_of_messages, current+1, current_offset);
                            MPI_Send(to_send, number_of_messages, message_type, current+1, 0,  second_comm);
                            second_topic_consumers[current].offset+=number_of_messages;
                            printf("\nsent %d messages to process %d, current_offset %d (second topic)", number_of_messages, current+1, second_topic_consumers[current].offset);   
                            free(to_send);
                        }     
                        
                        
                        if(second_topic_consumers[current].offset == 5){
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