package org.sndo.data.sdk.java.consumer;

import java.util.Map;

public interface Consumer {
    void send(Map<String, Object> message);

    void flush();

    void close();
}
