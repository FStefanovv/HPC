#include "consumer.h"

Consumer new_consumer() {
    Consumer consumer;
    consumer.offset = 0;

    return consumer;
}
