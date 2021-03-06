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

import it.ecubecenter.processors.zookeeper.utils.ThreadsafeZookeeperClient;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"zookeeper","znode","create"})
@CapabilityDescription("The processor write data to a znode. By default, it creates the znode "
        +"(this behavior can be changed with the proper property).")
@SeeAlso({ZookeeperList.class, ZookeeperDelete.class, ZookeeperReader.class,ZookeeperWriter.class})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ZookeeperCreate extends AbstractZookeeperProcessor {

    public static final PropertyDescriptor FAIL_IF_EXISTS = new PropertyDescriptor
            .Builder().name("Fail if zNode exists")
            .description("If the property is set to true, the flow file will be routed to the znode_existing relationship if zNode already exists.")
            .required(true)
            .allowableValues("true","false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor ZNODE_NAME = new PropertyDescriptor
            .Builder().name("ZNode")
            .description("The ZNode to be read")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INPUT_DATA = new PropertyDescriptor
            .Builder().name("Data to be written")
            .description("The data to be written in the specified zNode.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    public static final Relationship ZNODE_EXISTING = new Relationship.Builder()
            .name("znode_existing")
            .description("When the ZNode exists, the flow file is routed via this relationship.")
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
        descriptors.add(FAIL_IF_EXISTS);
        descriptors.add(INPUT_DATA);
        descriptors.addAll(properties);
        this.properties = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(ZNODE_EXISTING);
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
        renewKerberosAuthenticationIfNeeded(context);
        String zookeeperURL = context.getProperty(ZOOKEEPER_URL).getValue();
        ThreadsafeZookeeperClient conn = ThreadsafeZookeeperClient.getConnection(zookeeperURL);
        int trials = 3;
        while(trials>0) {
            try {
                String zNode = context.getProperty(ZNODE_NAME).evaluateAttributeExpressions(flowFile).getValue();
                String data = context.getProperty(INPUT_DATA).evaluateAttributeExpressions(flowFile).getValue();
                boolean failIfExists = context.getProperty(FAIL_IF_EXISTS).asBoolean();
                if (getLogger().isDebugEnabled())
                    getLogger().debug("Creating zNode " + zNode + " (with fail if exists to " + failIfExists + ") : " + data);
                boolean flag = conn.createZNode(zNode,
                        data.getBytes(),
                        failIfExists);

                if (flag) {
                    session.getProvenanceReporter().send(flowFile, zookeeperURL + zNode);
                    session.transfer(flowFile, SUCCESS);
                } else {
                    session.getProvenanceReporter().route(flowFile, ZNODE_EXISTING);
                    session.transfer(flowFile, ZNODE_EXISTING);
                }
                break;
            } catch (Exception e) {
                try {
                    conn.close();
                } catch (InterruptedException e1) {
                    getLogger().warn("Unable to close connection to Zookeeper.", e);
                }
                getLogger().warn("Error in writing to zookeeper.", e);

            }
            trials--;
        }
        if(trials == 0){
            getLogger().error("Zookeeper operation failed 3 times");
            session.transfer(flowFile, FAILURE);
        }
    }
}
