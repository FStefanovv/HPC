#ifndef CONSUMER_H
#define CONSUMER_H

typedef struct {
    int offset;
} Consumer;

Consumer new_consumer();

#endif