package org.sndo.data.sdk.java.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sndo.data.sdk.java.util.ObjectMapperUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BatchConsumer implements Consumer {

    public BatchConsumer(final String serverUrl) {
        this(serverUrl, MAX_FLUSH_BULK_SIZE);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize) {
        this(serverUrl, bulkSize, false);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final boolean throwException) {
        this.messageList = new LinkedList<Map<String, Object>>();
        this.httpConsumer = new HttpConsumer(serverUrl, null);
        this.jsonMapper = ObjectMapperUtils.getJsonObjectMapper();
        this.bulkSize = Math.min(MAX_FLUSH_BULK_SIZE, bulkSize);
        this.throwException = throwException;
    }

    @Override public void send(Map<String, Object> message) {
        synchronized (messageList) {
            messageList.add(message);
            if (messageList.size() >= bulkSize) {
                flush();
            }
        }
    }

    @Override public void flush() {
        synchronized (messageList) {
            while (!messageList.isEmpty()) {
                String sendingData = null;
                List<Map<String, Object>> sendList =
                        messageList.subList(0, Math.min(bulkSize, messageList.size()));
                try {
                    sendingData = jsonMapper.writeValueAsString(sendList);
                } catch (JsonProcessingException e) {
                    sendList.clear();
                    if (throwException) {
                        throw new RuntimeException("Failed to serialize data.", e);
                    }
                    continue;
                }

                try {
                    this.httpConsumer.consume(sendingData);
                    sendList.clear();
                } catch (Exception e) {
                    if (throwException) {
                        throw new RuntimeException("Failed to dump message with BatchConsumer.", e);
                    }
                    return;
                }
            }
        }
    }

    @Override public void close() {
        flush();
        httpConsumer.close();
    }

    private static final int MAX_FLUSH_BULK_SIZE = 50;

    private final List<Map<String, Object>> messageList;
    private final HttpConsumer httpConsumer;
    private final ObjectMapper jsonMapper;
    private final int bulkSize;
    private final boolean throwException;
}
