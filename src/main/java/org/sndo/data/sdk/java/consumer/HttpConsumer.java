package org.sndo.data.sdk.java.consumer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.sndo.data.sdk.java.SndoDataAnalytics;
import org.sndo.data.sdk.java.util.Base64Coder;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class HttpConsumer implements Closeable {

    private CloseableHttpClient httpClient;
    private final String serverUrl;
    private final Map<String, String> httpHeaders;

    public HttpConsumer(String serverUrl, Map<String, String> httpHeaders) {
        this.serverUrl = serverUrl;
        this.httpHeaders = httpHeaders;
        initClient();
    }

    private void initClient() {
        if (this.httpClient == null) {
            synchronized (this) {
                if (this.httpClient == null) {
                    this.httpClient = HttpClients.custom()
                            .setUserAgent("SndoDataAnalytics Java SDK " + SndoDataAnalytics.SDK_VERSION)
                            .setMaxConnTotal(100)
                            .build();
                }
            }
        }
    }

    public void consume(final String data) throws IOException, HttpConsumerException {
        HttpUriRequest request = getHttpRequest(data);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(request);
            int httpStatusCode = response.getStatusLine().getStatusCode();
            if (httpStatusCode < 200 || httpStatusCode >= 300) {
                String httpContent = new String(EntityUtils.toByteArray(response.getEntity()), "UTF-8");
                throw new HttpConsumerException(
                        String.format("Unexpected response %d from Sndo Data Analytics: %s", httpStatusCode, httpContent), data,
                        httpStatusCode, httpContent);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    HttpUriRequest getHttpRequest(final String data) throws IOException {
        HttpPost httpPost = new HttpPost(this.serverUrl);
        httpPost.setEntity(getHttpEntry(data));

        if (this.httpHeaders != null) {
            for (Map.Entry<String, String> entry : this.httpHeaders.entrySet()) {
                httpPost.addHeader(entry.getKey(), entry.getValue());
            }
        }

        return httpPost;
    }

    StringEntity getHttpEntry(final String data) throws IOException {
        byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
        ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(bytes);
        gos.close();
        byte[] compressed = os.toByteArray();
        os.close();
        return new StringEntity(new String(Base64Coder.encode(compressed)));
    }

    @Override
    public void close() {
        try {
            if (httpClient != null) {
                synchronized (this) {
                    if (httpClient != null) {
                        httpClient.close();
                        httpClient = null;
                    }
                }
            }
        } catch (IOException ignored) {
            // do nothing
        }
    }

    static class HttpConsumerException extends Exception {

        HttpConsumerException(String error, String sendingData, int httpStatusCode, String
                httpContent) {
            super(error);
            this.sendingData = sendingData;
            this.httpStatusCode = httpStatusCode;
            this.httpContent = httpContent;
        }

        String getSendingData() {
            return sendingData;
        }

        int getHttpStatusCode() {
            return httpStatusCode;
        }

        String getHttpContent() {
            return httpContent;
        }

        final String sendingData;
        final int httpStatusCode;
        final String httpContent;
    }

}
