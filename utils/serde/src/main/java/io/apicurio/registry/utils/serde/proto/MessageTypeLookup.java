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
import com.google.protobuf.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Tomer Rothschild (https://codeburst.io/protocol-buffers-part-3-json-format-e1ca0af27774)
 * @author Ales Justin
 */
public class MessageTypeLookup {

    private final Map<String, Supplier<Message.Builder>> _messageBuilderSupplierLookupMap;

    private MessageTypeLookup(final Map<String, Supplier<Message.Builder>> messageBuilderSupplierLookupMap) {
        _messageBuilderSupplierLookupMap = messageBuilderSupplierLookupMap;
    }

    Supplier<Message.Builder> getBuilderForTypeUrl(final String fullProtobufTypeUrl) {
        return _messageBuilderSupplierLookupMap.get(getTypeUrlSuffix(fullProtobufTypeUrl));
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private static String getTypeUrlSuffix(final String fullProtobufTypeUrl) {
        // removing the type url's prefix. see - https://developers.google.com/protocol-buffers/docs/proto3#any
        String[] splits = fullProtobufTypeUrl.split("/");
        return splits[splits.length - 1];
    }

    public static class Builder {
        private Map<String, Supplier<Message.Builder>> _messageBuilderMap;

        public Builder() {
            _messageBuilderMap = new HashMap<>();
        }

        public Builder addMessageTypeMapping(final Descriptors.Descriptor messageTypeDescriptor,
                                             final Supplier<Message.Builder> messageBuilderSupplier) {
            _messageBuilderMap.put(messageTypeDescriptor.getFullName(), messageBuilderSupplier);

            return this;
        }

        public MessageTypeLookup build() {
            return new MessageTypeLookup(_messageBuilderMap);
        }
    }
}
