/*
 * Copyright 2019 Red Hat
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

package io.apicurio.registry.utils.serde.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * @author Tomer Rothschild (https://codeburst.io/protocol-buffers-part-3-json-format-e1ca0af27774)
 * @author Ales Justin
 */
public class AnyJsonPacker {

    // unless you host a schema repository like https://github.com/spotify/proto-registry then
    // this prefix is just a place holder:
    private static final String COMPANY_TYPE_URL_PREFIX = "type.apicuiro.io";
    private static final JsonFormat.Printer _jsonPrinter = JsonFormat.printer();

    public static Any.Json pack(final Message message) throws InvalidProtocolBufferException {
        String typeUrl = getTypeUrl(message.getDescriptorForType());
        String jsonSerializedValue = _jsonPrinter.print(message);

        return Any.Json.newBuilder()
                       .setTypeUrl(typeUrl)
                       .setJsonSerializedValue(jsonSerializedValue)
                       .build();
    }

    private static String getTypeUrl(final Descriptors.Descriptor descriptor) {
        return COMPANY_TYPE_URL_PREFIX + "/" + descriptor.getFullName();
    }
}
