package org.sndo.data.sdk.java.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.utils.URIBuilder;
import org.sndo.data.sdk.java.exceptions.DebugModeException;
import org.sndo.data.sdk.java.util.ObjectMapperUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DebugConsumer implements Consumer {

    public DebugConsumer(final String serverUrl, final boolean writeData) {
        String debugUrl = null;
        try {
            // 将 URI Path 替换成 Debug 模式的 '/debug'
            URIBuilder builder = new URIBuilder(new URI(serverUrl));
            String[] urlPathes = builder.getPath().split("/");
            urlPathes[urlPathes.length - 1] = "debug";
            builder.setPath(strJoin(urlPathes, "/"));
            debugUrl = builder.build().toURL().toString();
        } catch (URISyntaxException e) {
            throw new DebugModeException(e);
        } catch (MalformedURLException e) {
            throw new DebugModeException(e);
        }

        Map<String, String> headers = new HashMap<String, String>();
        if (!writeData) {
            headers.put("Dry-Run", "true");
        }

//        this.httpConsumer = new HttpConsumer(debugUrl, headers);
        this.httpConsumer = new HttpConsumer(serverUrl, headers);
        this.jsonMapper = ObjectMapperUtils.getJsonObjectMapper();
    }

    @Override public void send(Map<String, Object> message) {
        // XXX: HttpConsumer 只处理了 Message List 的发送？
        List<Map<String, Object>> messageList = new ArrayList<Map<String, Object>>();
        messageList.add(message);

        String sendingData = null;
        try {
            sendingData = jsonMapper.writeValueAsString(messageList);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize data.", e);
        }

        System.out.println("==========================================================================");

        try {
            httpConsumer.consume(sendingData);
            System.out.println(String.format("valid message: %s", sendingData));
        } catch (IOException e) {
            throw new DebugModeException("Failed to send message with DebugConsumer.", e);
        } catch (HttpConsumer.HttpConsumerException e) {
            System.out.println(String.format("invalid message: %s", e.getSendingData()));
            System.out.println(String.format("http status code: %d", e.getHttpStatusCode()));
            System.out.println(String.format("http content: %s", e.getHttpContent()));
            throw new DebugModeException(e);
        }
    }

    @Override public void flush() {
        // do NOTHING
    }

    @Override public void close() {
        httpConsumer.close();
    }

    final HttpConsumer httpConsumer;
    final ObjectMapper jsonMapper;

    private static String strJoin(String[] arr, String sep) {
        StringBuilder sbStr = new StringBuilder();
        for (int i = 0, il = arr.length; i < il; i++) {
            if (i > 0)
                sbStr.append(sep);
            sbStr.append(arr[i]);
        }
        return sbStr.toString();
    }
}
