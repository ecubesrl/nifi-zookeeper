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
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"zookeeper","znode","delete"})
@CapabilityDescription("The processor write data to a znode. By default, it creates the znode "
        +"(this behavior can be changed with the proper property).")
@SeeAlso({ZookeeperList.class, ZookeeperWriter.class, ZookeeperReader.class})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ZookeeperDelete extends AbstractZookeeperProcessor {

    public static final PropertyDescriptor ZNODE_NAME = new PropertyDescriptor
            .Builder().name("ZNode")
            .description("The ZNode to be deleted")
            .required(true)
            .expressionLanguageSupported(true)
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
        renewKerberosAuthenticationIfNeeded(context);
        String zookeeperURL = context.getProperty(ZOOKEEPER_URL).getValue();
        ThreadsafeZookeeperClient conn = ThreadsafeZookeeperClient.getConnection(zookeeperURL);
        int trials = 3;
        while(trials>0) {
            try {
                String zNode = context.getProperty(ZNODE_NAME).evaluateAttributeExpressions(flowFile).getValue();
                boolean flag = conn.deleteZNode(zNode);

                if(flag){
                    session.getProvenanceReporter().send(flowFile, zookeeperURL + zNode);
                    session.transfer(flowFile, SUCCESS);
                }else {
                    session.getProvenanceReporter().route(flowFile, ZNODE_NOT_FOUND);
                    session.transfer(flowFile,ZNODE_NOT_FOUND);
                }
                break;
            } catch (Exception e) {
                try {
                    conn.close();
                } catch (InterruptedException e1) {
                    getLogger().warn("Unable to close connection to Zookeeper.", e);
                }
                getLogger().warn("Error in deleting zNode.", e);
            }
            trials--;
        }
        if(trials == 0){
            getLogger().error("Zookeeper operation failed 3 times");
            session.transfer(flowFile, FAILURE);
        }
    }
}
