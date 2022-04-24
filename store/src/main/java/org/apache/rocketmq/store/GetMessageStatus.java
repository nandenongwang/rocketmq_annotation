package org.apache.rocketmq.store;

public enum GetMessageStatus {

    FOUND,

    NO_MATCHED_MESSAGE,

    MESSAGE_WAS_REMOVING,

    OFFSET_FOUND_NULL,

    OFFSET_OVERFLOW_BADLY,

    OFFSET_OVERFLOW_ONE,

    OFFSET_TOO_SMALL,

    NO_MATCHED_LOGIC_QUEUE,

    NO_MESSAGE_IN_QUEUE,
}
