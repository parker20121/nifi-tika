package com.giantoak.nifi.processor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.ToXMLContentHandler;
import org.xml.sax.ContentHandler;

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"Tika", "Extract Text"})
@CapabilityDescription("Extract text and metadata from file and store in XPATH file")
@SupportsBatching
public class TikaProcessor extends AbstractProcessor {

    public static final PropertyDescriptor INPUT_FILE_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("Input File Attribute")
        .required(true)
        .description("")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor OVERWRITE_FILES = new PropertyDescriptor.Builder()
        .name("Overwrite Files")
        .required(false)
        .description("")
        .allowableValues("True", "False")
        .defaultValue("False")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();
    
    //public static final PropertyDescriptor OUTPUT_FILE_PATH = new PropertyDescriptor.Builder()
    //    .name("Output File Path")
    //   .required(true)
    //    .description("")
    //    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
   //     .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that successfully has media metadata extracted will be routed to success")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that fails to have media metadata extracted will be routed to failure")
            .build();

    private volatile ContentHandler handler = null;
    private volatile AutoDetectParser parser = null;
    
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(ProcessorInitializationContext context) {
        
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(INPUT_FILE_ATTRIBUTE);
        properties.add(OVERWRITE_FILES);
        //properties.add(OUTPUT_FILE_PATH);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

    }
   
    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }
    
    @SuppressWarnings("unused")
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        handler = new ToXMLContentHandler(); 
        parser = new AutoDetectParser();  
    }
    
    @Override
    @SuppressWarnings("UseSpecificCatch")
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
       
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = this.getLogger();

        String inputFileAttribute = context.getProperty(INPUT_FILE_ATTRIBUTE).getValue();
        String inputFileName = flowFile.getAttribute(inputFileAttribute);
        String outputFileName = inputFileName + ".xhtml";
        
        File outputFile = new File(outputFileName);
        
        if ( outputFile.exists() ){
            if ( context.getProperty(OVERWRITE_FILES).asBoolean() ){
                outputFile.delete();
            } else {
                session.transfer(flowFile, SUCCESS);
                return;
            }
        }
        
        Metadata metadata = new Metadata();
        
        try {            
            InputStream stream = new FileInputStream(inputFileName);
            parser.parse(stream, handler, metadata);
            String content = handler.toString();
            FileUtils.writeStringToFile( outputFile, content );           
        } catch ( Exception e ){
            throw new ProcessException(e);
        }
    
        // Write the results to attributes
        Map<String, String> results = value.get();
        if (results != null && !results.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, results);
        }
        session.transfer(flowFile, SUCCESS);
        
    }

}
