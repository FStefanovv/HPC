#include "topic_queue.h"

TopicQueue* new_queue(char *name) {
    TopicQueue* queue = (TopicQueue*)malloc(sizeof(TopicQueue));
    queue->current = 0;
    queue->name = name;

    return queue;
}


void enqueue(Message message, TopicQueue *queue) {
    int position = queue->current;
    message.id = position;
    queue->messages[position] = message;
    (queue->current)++;
}


Message* readMessages(int offset, int* size, TopicQueue queue) {

    if(queue.current==0 || offset >= queue.current) {
        return NULL;
    }

    *size = queue.current - offset;
    
    Message *messages = (Message*)malloc(sizeof(Message) * *size);

    for(int i=offset; i < queue.current; i++){
        messages[i-offset] = queue.messages[i];
    }

    return messages;
}

void print_topic(TopicQueue queue) {
    printf("\n\ntopic %s", queue.name);
    for(int i=0; i<queue.current; i++){
        printf("\n%d: %s", queue.messages[i].id, queue.messages[i].content);
    }
}
