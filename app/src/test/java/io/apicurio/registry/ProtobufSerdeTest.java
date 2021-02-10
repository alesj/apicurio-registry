/*
 * Copyright 2021 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.apicurio.registry;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.apicurio.registry.support.HeaderOuterClass;
import io.apicurio.registry.support.Ptc2080;
import io.apicurio.registry.utils.serde.ProtobufKafkaDeserializer;
import io.apicurio.registry.utils.serde.ProtobufKafkaSerializer;
import io.apicurio.registry.utils.serde.SerdeConfig;
import io.apicurio.registry.utils.serde.strategy.GetOrCreateIdStrategy;
import io.apicurio.registry.utils.serde.util.HeaderUtils;
import io.apicurio.registry.utils.serde.util.Utils;
import io.apicurio.registry.utils.tests.TestUtils;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.apicurio.registry.utils.serde.SerdeConfig.USE_HEADERS;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Ales Justin
 * @author Steve Collins (GitHub issue 1186)
 */
@QuarkusTest
public class ProtobufSerdeTest extends AbstractResourceTestBase {

    @Test
    void testIssue1886() {
        Map<String, Object> producerParams = new HashMap<>();
        producerParams.put(SerdeConfig.REGISTRY_URL, TestUtils.getRegistryApiUrl());
        producerParams.put(SerdeConfig.GLOBAL_ID_STRATEGY, GetOrCreateIdStrategy.class);
        producerParams.put(USE_HEADERS, true); //global Id location
        ProtobufKafkaSerializerWithHeaders<Ptc2080> serializer = new ProtobufKafkaSerializerWithHeaders<>();
        serializer.configure(producerParams, false);

        Map<String, Object> consumerParams = new HashMap<>();
        consumerParams.put(SerdeConfig.REGISTRY_URL, TestUtils.getRegistryApiUrl());
        consumerParams.put(USE_HEADERS, true); //global Id location
        ProtobufKafkaDeserializerWithHeaders deserializer = new ProtobufKafkaDeserializerWithHeaders();
        deserializer.configure(consumerParams, false);

        Ptc2080 record = Ptc2080
                .newBuilder()
                .setI1(1)
                .setI2(1)
                .setI4(3)
                .setI5(4333)
                .setHeader(HeaderOuterClass.Header.newBuilder().setMessageTypeId("ptc2080").setUuid("12345").build())
                .build();

        Headers headers = new RecordHeaders();
        byte[] bytes = serializer.serialize("i1186", headers, record);
        //this line will fail
        final DynamicMessage deserialize = deserializer.deserialize("i1186", headers, bytes);
        assertNotNull(deserialize, "deserialized i1186 must not be null");
    }

    static class ProtobufKafkaDeserializerWithHeaders extends ProtobufKafkaDeserializer {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            super.configure(configs, isKey);
            if (Utils.isTrue(configs.get(USE_HEADERS))) {
                //noinspection unchecked
                headerUtils = new HeaderUtils((Map<String, Object>) configs, isKey);
            }
        }
    }

    static class ProtobufKafkaSerializerWithHeaders<U extends Message> extends ProtobufKafkaSerializer<U> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            super.configure(configs, isKey);
            if (Utils.isTrue(configs.get(USE_HEADERS))) {
                //noinspection unchecked
                headerUtils = new HeaderUtils((Map<String, Object>) configs, isKey);
            }
        }
    }

}
