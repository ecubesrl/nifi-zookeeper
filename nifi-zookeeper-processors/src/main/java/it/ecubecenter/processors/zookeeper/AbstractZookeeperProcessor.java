package it.ecubecenter.processors.zookeeper;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.KerberosTicketRenewer;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import sun.security.krb5.internal.util.KerberosString;

import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by gaido on 22/11/2016.
 */
public abstract class AbstractZookeeperProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ZOOKEEPER_URL = new PropertyDescriptor
            .Builder().name("Zookeeper URL")
            .description("Zookeeper connections string")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Object RESOURCES_LOCK = new Object();
    private static UserGroupInformation ugi = null;

    protected KerberosProperties kerberosProperties;
    protected List<PropertyDescriptor> properties;
    private volatile File kerberosConfigFile = null;

    @Override
    protected void init(ProcessorInitializationContext context) {

        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosProperties = getKerberosProperties(kerberosConfigFile);

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(kerberosProperties.getKerberosPrincipal());
        props.add(kerberosProperties.getKerberosKeytab());
        props.add(ZOOKEEPER_URL);
        properties = Collections.unmodifiableList(props);
    }

    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return new KerberosProperties(kerberosConfigFile);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if( ( descriptor.equals(kerberosProperties.getKerberosKeytab())
                || descriptor.equals(kerberosProperties.getKerberosPrincipal()) )
                && !oldValue.equals(newValue)) {
            synchronized (RESOURCES_LOCK){
                ugi = null;
            }
        }
        super.onPropertyModified(descriptor, oldValue, newValue);
    }

    protected void renewKerberosAuthenticationIfNeeded(ProcessContext context) {

        try {
            String keytabPath = context.getProperty(kerberosProperties.getKerberosKeytab()).getValue();
            String kerberosPrincipal = context.getProperty(kerberosProperties.getKerberosPrincipal()).getValue();
            if(keytabPath == null || kerberosPrincipal == null || keytabPath.equals("") || kerberosPrincipal.equals("")){
                getLogger().info("Kerberos is not enabled, since principal and/or keytab properties are empty.");
                return;
            }
            synchronized (RESOURCES_LOCK) {
                if(ugi == null){
                    getLogger().info("Attempting to login user {} from keytab {}", new Object[]{kerberosPrincipal, keytabPath});
                    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosPrincipal, keytabPath);
                }else{
                    ugi.checkTGTAndReloginFromKeytab();
                }
            }
            getLogger().info("Kerberos relogin successful or ticket still valid");
        } catch (IOException e) {
            // Most likely case of this happening is ticket is expired and error getting a new one,
            // meaning dfs operations would fail
            getLogger().error("Kerberos relogin failed", e);
            throw new ProcessException("Unable to renew kerberos ticket", e);
        }
    }


}