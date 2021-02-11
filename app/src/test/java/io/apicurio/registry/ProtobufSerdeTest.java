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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.apicurio.registry.client.RegistryRestClient;
import io.apicurio.registry.client.RegistryRestClientFactory;
import io.apicurio.registry.common.proto.Serde;
import io.apicurio.registry.support.TestCmmn;
import io.apicurio.registry.utils.IoUtil;
import io.apicurio.registry.utils.serde.AbstractKafkaSerDe;
import io.apicurio.registry.utils.serde.AbstractKafkaStrategyAwareSerDe;
import io.apicurio.registry.utils.serde.ProtobufKafkaDeserializer;
import io.apicurio.registry.utils.serde.ProtobufKafkaSerializer;
import io.apicurio.registry.utils.serde.strategy.GetOrCreateIdStrategy;
import io.apicurio.registry.utils.serde.util.HeaderUtils;
import io.apicurio.registry.utils.serde.util.Utils;
import io.apicurio.registry.utils.tests.TestUtils;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Ales Justin
 * @author Steve Collins (GitHub issue 1186)
 */
@QuarkusTest
public class ProtobufSerdeTest extends AbstractResourceTestBase {

    @Test
    void shouldSerdeTestMessage() throws Exception {
        //configure producer
        Map<String, Object> producerParams = new HashMap<>();
        producerParams.put(AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, TestUtils.getRegistryApiUrl());
        producerParams.put(AbstractKafkaStrategyAwareSerDe.REGISTRY_GLOBAL_ID_STRATEGY_CONFIG_PARAM, GetOrCreateIdStrategy.class);
        producerParams.put(AbstractKafkaSerDe.USE_HEADERS, true); //global Id location

        ProtobufKafkaSerializerWithHeaders<TestCmmn.UUID> serializer = new ProtobufKafkaSerializerWithHeaders<>();
        serializer.configure(producerParams, false);

        //configure consumer
        Map<String, Object> consumerParams = new HashMap<>();
        consumerParams.put(AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, TestUtils.getRegistryApiUrl());
        consumerParams.put(AbstractKafkaSerDe.USE_HEADERS, true); //global Id location

        ProtobufKafkaDeserializerWithHeaders deserializer = new ProtobufKafkaDeserializerWithHeaders();
        deserializer.configure(consumerParams, false);

        TestCmmn.UUID testCmmn = TestCmmn.UUID
                .newBuilder()
                .setLsb(1)
                .setMsb(2)
                .build();

        Serde.Schema expected_schemaFromFileDescriptor = toSchemaProto(testCmmn.getDescriptorForType().getFile());
        byte[] binarySchemaFromJavaProtoObj = expected_schemaFromFileDescriptor.toByteArray();
        String schemaStringFromJavaObject = new String(binarySchemaFromJavaProtoObj);
        Serde.Schema parsedSchemaFromJavaProtoObject = Serde.Schema.parseFrom(binarySchemaFromJavaProtoObj);
        assertEquals(expected_schemaFromFileDescriptor, parsedSchemaFromJavaProtoObject, "expected_schemaFromFileDescriptor from the java object and the parsed expected_schemaFromFileDescriptor from the binary should equal");

        Headers headers = new RecordHeaders();
        final byte[] serializedData = serializer.serialize("com.csx.testcmmn", headers, testCmmn);

        byte[] queriedSchema;
        HeaderUtils headerUtils = new HeaderUtils(consumerParams, false);

        try (RegistryRestClient restClient = RegistryRestClientFactory.create(TestUtils.getRegistryApiUrl())) {
            Long id = headerUtils.getGlobalId(headers);
            InputStream artifactResponse = restClient.getArtifactByGlobalId(id);
            queriedSchema = IoUtil.toBytes(artifactResponse);
        }
        //test still fails here
        Serde.Schema queriedSchemaObj = Serde.Schema.parseFrom(queriedSchema);
        assertEquals(expected_schemaFromFileDescriptor, queriedSchemaObj, "expected_schemaFromFileDescriptor from the java object and the parsed queriedSchemaObj from the web api call should equal");

        assertEquals(schemaStringFromJavaObject, new String(queriedSchema), "the queried binarySchemaFromJavaProtoObj must match the binarySchemaFromJavaProtoObj from the proto java object file descriptor");

        final DynamicMessage deserialize = deserializer.deserialize("com.csx.testcmmn", headers, serializedData);

        assertNotNull(deserialize, "deserialized 2080 must not be null");

    }

    private Serde.Schema toSchemaProto(Descriptors.FileDescriptor file) {
        Serde.Schema.Builder b = Serde.Schema.newBuilder();
        b.setFile(file.toProto());
        for (Descriptors.FileDescriptor d : file.getDependencies()) {
            b.addImport(toSchemaProto(d));
        }
        return b.build();
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
