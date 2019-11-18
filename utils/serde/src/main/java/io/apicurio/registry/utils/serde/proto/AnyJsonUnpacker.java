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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * @author Tomer Rothschild (https://codeburst.io/protocol-buffers-part-3-json-format-e1ca0af27774)
 * @author Ales Justin
 */
public class AnyJsonUnpacker {
    private final JsonFormat.Parser jsonParser;
    private final MessageTypeLookup _messageTypeLookup;

    public AnyJsonUnpacker(final MessageTypeLookup messageTypeLookup) {
        _messageTypeLookup = messageTypeLookup;
        jsonParser = JsonFormat.parser();
    }

    public Message unpack(final Any.Json anyJsonMessage) throws InvalidProtocolBufferException {
        Message.Builder messageBuilder = _messageTypeLookup.getBuilderForTypeUrl(anyJsonMessage.getTypeUrl())
                                                           .get();

        jsonParser.merge(anyJsonMessage.getJsonSerializedValue(), messageBuilder);

        return messageBuilder.build();
    }
}
