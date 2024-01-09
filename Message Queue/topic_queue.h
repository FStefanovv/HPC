#ifndef TOPIC_QUEUE_H
#define TOPIC_QUEUE_H

#define MAX_MESSAGES 50

#include "message_type.h"
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    Message messages[MAX_MESSAGES];
    int current;
    char *name;
} TopicQueue;


TopicQueue *new_queue(char*);

void enqueue(Message, TopicQueue*);

Message* readMessages(int, int*, TopicQueue);

void print_topic(TopicQueue);

#endif