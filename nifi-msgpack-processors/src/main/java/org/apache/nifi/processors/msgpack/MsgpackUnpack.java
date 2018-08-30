/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.msgpack;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;
import java.util.HashMap;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePack.PackerConfig;
import org.msgpack.core.MessagePack.UnpackerConfig;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueType;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.*;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"msgpack", "json"})
@CapabilityDescription("Unpack values from msgpack to json")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MsgpackUnpack extends AbstractProcessor {

   public static final String FORMAT = "Json";

    public static final PropertyDescriptor ACTION_PROPERTY = new PropertyDescriptor
            .Builder().name("FORMAT")
            .displayName("Format")
            .description("Set format to unpack")
            .required(true)
            .defaultValue(FORMAT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(FORMAT)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully unpacked will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format, the unchanged FlowFile will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ACTION_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException  {
                    
                    Gson gson = new Gson();
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(IOUtils.toByteArray(in));
                    // Map<String, Object> map = new HashMap<String, Object>();

                    Object map;
                    map = unpack(unpacker);

                    String json = gson.toJson(map); 
                    
                    // Debug
                    String prettyJson = toPrettyFormat(json);
                    System.out.println("Pretty:\n" + prettyJson);

                    unpacker.close();
                    value.set(json);
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as JSON due to {}; routing to failure", new Object[] {flowFile, pe.toString()}, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        };
        

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        session.transfer(flowFile,REL_SUCCESS);
    }


    static Object unpack(MessageUnpacker unpacker) throws IOException  {
        MessageFormat format = unpacker.getNextFormat();
        ValueType valueType = format.getValueType(); 
        switch ( valueType ) {
                case BOOLEAN:
                    return unpacker.unpackBoolean();
                case INTEGER:
                    switch (format) {
                    case UINT64:
                        return unpacker.unpackBigInteger();
                    case INT64:
                    case UINT32:
                        return unpacker.unpackLong();
                    default:
                        return unpacker.unpackInt();
                    }
                case FLOAT:
                    return unpacker.unpackFloat();
                case STRING:
                    return unpacker.unpackString();
                case BINARY:
                    return unpacker.unpackString();
                case ARRAY:
                    return unpackArray(unpacker);
                case MAP:
                    return unpackmap(unpacker);
                default:
                    System.out.println("Unsupported ValueType in MsgPack: "+valueType);
                    return null;
            }

    }

    static Map<String, Object> unpackmap(MessageUnpacker unpacker) 
        throws IOException  {
        
        Map<String, Object> data = new HashMap<String, Object>();
        int length = unpacker.unpackMapHeader();
        for (int i = 0; i < length; i++) {
            String key = unpacker.unpackString();

            Object value;
        
            value = unpack(unpacker);
            data.put(key, value);
        }

        return data;
       
    }
    
    static List<Object> unpackArray(MessageUnpacker unpacker) throws IOException  {
       
        int length = unpacker.unpackArrayHeader();
        List<Object> myList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            myList.add(unpack(unpacker));
        }

        return myList;
    }
    
    protected static String toPrettyFormat(String jsonString) {
        JsonParser parser = new JsonParser();
        JsonElement json = parser.parse(jsonString);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String prettyJson = gson.toJson(json);

        return prettyJson;
    }
}
