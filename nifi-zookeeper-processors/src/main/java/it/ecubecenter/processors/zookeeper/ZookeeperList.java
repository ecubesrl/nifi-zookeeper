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
package it.ecubecenter.processors.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.ecubecenter.processors.zookeeper.utils.ThreadsafeZookeeperClient;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedOutputStream;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.OutputStream;
import java.util.*;

@Tags({"zookeeper","list","znode"})
@CapabilityDescription("The processor lists the content of the provided zNode and returns the list of the children in the content of the FlowFile.")
@SeeAlso({ZookeeperWriter.class, ZookeeperDelete.class, ZookeeperReader.class})
@WritesAttribute(attribute = ZookeeperList.ATTIRUBTE_NAME,
        description = "The attribute in which the result is stored if the chosen destination is "+ZookeeperList.DESTINATION_CONTENT)
public class ZookeeperList extends AbstractZookeeperProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String ATTIRUBTE_NAME = "zookeeper.list";

    private static final ObjectMapper objMapper = new ObjectMapper();
    private static Map<String, String> MIME_TYPE_MAP;
    static
    {
        MIME_TYPE_MAP = new HashMap<String, String>();
        MIME_TYPE_MAP.put("csv", "application/csv");
        MIME_TYPE_MAP.put("json", "application/json");
    }

    public static final PropertyDescriptor ZNODE_NAME = new PropertyDescriptor
            .Builder().name("ZNode")
            .description("The ZNode to be listed")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor
            .Builder().name("Destination")
            .description("Control if value is written as a new flowfile attribute '" + ATTIRUBTE_NAME + "' " +
                    "or written in the flowfile content. Writing to flowfile content will overwrite any " +
                    "existing flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE,DESTINATION_CONTENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor
            .Builder().name("Output format")
            .description("The format of the output of the operation.")
            .required(true)
            .allowableValues("csv","json")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    public static final Relationship ZNODE_NOT_FOUND = new Relationship.Builder()
            .name("znode_not_found")
            .description("When the ZNode does not exists the flow file is routed via this relationship.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();


    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ZNODE_NAME);
        descriptors.add(OUTPUT_FORMAT);
        descriptors.add(DESTINATION);
        descriptors.addAll(properties);
        this.properties = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(ZNODE_NOT_FOUND);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final String outputFormat  = context.getProperty(OUTPUT_FORMAT).getValue();
        renewKerberosAuthenticationIfNeeded(context);
        String zookeeperURL = context.getProperty(ZOOKEEPER_URL).getValue();
        ThreadsafeZookeeperClient conn = ThreadsafeZookeeperClient.getConnection(zookeeperURL);
        int trials = 3;
        while(trials>0) {
            try {
                String zNode = context.getProperty(ZNODE_NAME).evaluateAttributeExpressions(flowFile).getValue();
                List<String> list = conn.listZNode(zNode);

                if(list == null){
                    session.getProvenanceReporter().route(flowFile, ZNODE_NOT_FOUND);
                    session.transfer(flowFile,ZNODE_NOT_FOUND);
                }else {
                    switch (context.getProperty(DESTINATION).getValue()){
                        case DESTINATION_CONTENT:
                            flowFile = session.write(flowFile, new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream outputStream) throws IOException {
                                    switch (outputFormat){
                                        case "csv":
                                            StringBuilder sb = new StringBuilder();
                                            boolean firstLine=true;
                                            for(String zNode: list){
                                                if(!firstLine){
                                                    sb.append(System.lineSeparator());
                                                }
                                                sb.append(zNode);
                                                firstLine=false;
                                            }
                                            outputStream.write(sb.toString().getBytes());
                                            break;
                                        case "json":
                                            outputStream.write(objMapper.writeValueAsBytes(list));
                                            break;
                                        default:
                                            throw new InvalidClassException("Unrecognized output format.");
                                    }

                                }
                            });
                            session.getProvenanceReporter().fetch(flowFile, zookeeperURL + zNode);
                            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), MIME_TYPE_MAP.get(outputFormat));
                            break;
                        case DESTINATION_ATTRIBUTE:
                            switch (outputFormat) {
                                case "csv":
                                    StringBuilder sb = new StringBuilder();
                                    boolean firstLine = true;
                                    for(String zN: list){
                                        if(!firstLine){
                                            sb.append(",");
                                        }
                                        sb.append(zN);
                                        firstLine=false;
                                    }
                                    flowFile = session.putAttribute(flowFile, ATTIRUBTE_NAME, sb.toString());
                                    break;
                                case "json":
                                    flowFile = session.putAttribute(flowFile, ATTIRUBTE_NAME, objMapper.writeValueAsString(list));
                                    break;
                                default:
                                    throw new InvalidClassException("Unrecognized output format.");
                            }
                            session.getProvenanceReporter().modifyAttributes(flowFile);
                            break;
                        default:
                            throw new InvalidClassException("Unrecognized destination.");

                    }


                    session.transfer(flowFile, SUCCESS);
                }

                break;
            } catch (Exception e) {
                try {
                    conn.close();
                } catch (InterruptedException e1) {
                    getLogger().warn("Unable to close connection to Zookeeper.", e);
                }
                getLogger().warn("Error while listing from Zookeeper", e);
            }
            trials--;
        }
        if(trials == 0){
            getLogger().error("Zookeeper operation failed 3 times");
            session.transfer(flowFile, FAILURE);
        }


    }
}
