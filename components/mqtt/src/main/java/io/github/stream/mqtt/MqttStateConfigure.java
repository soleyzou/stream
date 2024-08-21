/**
 * Copyright wendy512@yeah.net
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.github.stream.mqtt;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import io.github.stream.core.Configurable;
import io.github.stream.core.StreamException;
import io.github.stream.core.configuration.ConfigContext;
import io.github.stream.core.properties.BaseProperties;
import io.github.stream.core.utils.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * 抽象mqtt管理
 * @author wendy512@yeah.net
 * @date 2023-05-23 15:00:12
 * @since 1.0.0
 */
@Slf4j
public final class MqttStateConfigure implements Configurable {

    public static final String OPTIONS_HOST = "host";
    public static final String OPTIONS_CLIENT_ID = "clientId";
    public static final String OPTIONS_TOPIC = "topic";
    public static final String OPTIONS_QOS = "qos";
    public static final String OPTIONS_CONNECT_TIMEOUT = "connectionTimeout";
    public static final String OPTIONS_KEEP_ALIVE_INTERVAL = "keepAlive";
    public static final String OPTIONS_CLEAN_SESSION = "cleanSession";
    public static final String OPTIONS_AUTOMATIC_RECONNECT = "autoReconnect";
    public static final String OPTIONS_USERNAME = "username";
    public static final String OPTIONS_PASSWORD = "password";
    public static final String OPTIONS_TIMETOWAIT = "timeToWait";
    public static final int DEFAULT_TIMETOWAIT = 60000;

    private static final Map<String, MqttStateConfigure> instances = new ConcurrentHashMap<>();

    private final AtomicBoolean configured = new AtomicBoolean(false);

    private MqttClient client;

    private int qos;

    private MqttStateConfigure() {}

    /**
     * 获取客户端实例，保证同一实例名只创建一个客户端，节省资源
     * @param name 实例名称
     * @return 创建后的客户端实例
     */
    public static MqttStateConfigure getInstance(String name) {
        MqttStateConfigure instance = instances.get(name);
        if (instance != null) {
            return instance;
        }

        synchronized (MqttStateConfigure.class) {
            // double check
            if (!instances.containsKey(name)) {
                instance = new MqttStateConfigure();
                instances.put(name, instance);
            } else {
                instance = instances.get(name);
            }
        }
        return instance;
    }

    @Override
    public void configure(ConfigContext context) throws Exception {
        if (!configured.compareAndSet(false, true)) {
            return;
        }
        BaseProperties properties = context.getInstance();
        MqttConnectOptions options = createOptions(properties);

        String host = properties.getString(OPTIONS_HOST);
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("MQTT host cannot be empty");
        }

        String clientId = context.getConfig().getString(OPTIONS_CLIENT_ID);
        if (StringUtils.isBlank(clientId)) {
            clientId = UUID.fastUUID().toString(true);
        }

        int timeToWait = properties.getInt(OPTIONS_TIMETOWAIT, DEFAULT_TIMETOWAIT);
        this.qos = properties.getInt(OPTIONS_QOS, 0);
        log.info("Connect to mqtt server {}, client id {}", host, clientId);
        try {
            this.client = new MqttClient(host, clientId, new MemoryPersistence());
            this.client.setTimeToWait(timeToWait);
            this.client.connect(options);
        } catch (MqttException e) {
            throw new StreamException(e);
        }
    }

    private MqttConnectOptions createOptions(BaseProperties properties) throws Exception {
        int connectionTimeout =
                properties.getInt(OPTIONS_CONNECT_TIMEOUT, MqttConnectOptions.CONNECTION_TIMEOUT_DEFAULT);
        int keepAlive = properties.getInt(OPTIONS_KEEP_ALIVE_INTERVAL,
                MqttConnectOptions.KEEP_ALIVE_INTERVAL_DEFAULT);
        boolean cleanSession =
                properties.getBooleanValue(OPTIONS_CLEAN_SESSION, MqttConnectOptions.CLEAN_SESSION_DEFAULT);
        boolean autoReconnect = properties.getBooleanValue(OPTIONS_AUTOMATIC_RECONNECT, true);
        String username = properties.getString(OPTIONS_USERNAME);
        String password = properties.getString(OPTIONS_PASSWORD);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setConnectionTimeout(connectionTimeout);
        options.setKeepAliveInterval(keepAlive);
        options.setCleanSession(cleanSession);
        options.setAutomaticReconnect(autoReconnect);
        if (StringUtils.isNotBlank(username)) {
            options.setUserName(username);
        }
        if (StringUtils.isNotBlank(password)) {
            options.setPassword(password.toCharArray());
        }

        setSSLOptions(properties, options);
        // SSL配置
        return options;
    }

    public void stop() {
        if (null != client && client.isConnected()) {
            try {
                client.disconnect();
                client.close();
            } catch (MqttException e) {
                throw new StreamException(e);
            }
        }
    }

    public MqttClient getClient() {
        return client;
    }

    public int getQos() {
        return qos;
    }

    private void setSSLOptions(BaseProperties properties, MqttConnectOptions options)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException {
        BaseProperties ssl = properties.getProperties("ssl");
        if (null != ssl && ssl.getBooleanValue("enabled")) {
            String keyStorePath = ssl.getString("keyStore");
            Assert.hasText(keyStorePath, "MQTT ssl key-store cannot be empty");

            // 加载信任库
            KeyStore trustStore = KeyStore.getInstance(ssl.getString("keyStoreType", "JKS"));
            InputStream keyStoreStream;
            String keyStorePassword = ssl.getString("keyStorePassword", "");

            if (keyStorePath.startsWith(ResourceUtils.CLASSPATH_URL_PREFIX)) {
                String path = keyStorePath.substring(ResourceUtils.CLASSPATH_URL_PREFIX.length());
                ClassPathResource resource = new ClassPathResource(path);
                keyStoreStream = resource.getInputStream();
            } else {
                keyStoreStream = new FileInputStream(keyStorePath);
            }

            trustStore.load(keyStoreStream, keyStorePassword.toCharArray());
            // 创建信任管理器工厂
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // 创建 SSL 上下文
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            options.setSocketFactory(sslContext.getSocketFactory());
        }
    }
}
