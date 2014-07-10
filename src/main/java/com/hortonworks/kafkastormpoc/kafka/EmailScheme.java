package com.hortonworks.kafkastormpoc.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.log4j.Logger;

import com.hortonworks.kafkastormpoc.common.EmailAttachment;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class EmailScheme implements Scheme{

	

	/**
	 * 
	 */
	private static final long serialVersionUID = 5010053689948707994L;
	private static final Logger LOG = Logger.getLogger(EmailScheme.class);
	
	public List<Object> deserialize(byte[] bytes) {
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		    ObjectInputStream is = new ObjectInputStream(in);
		    Object obj = is.readObject();
		    if ( obj instanceof EmailAttachment)
		    {
			    EmailAttachment a = (EmailAttachment) obj; 
			    int emailId = a.getEmailId();
			    
			
			    LOG.info("Creating an email Scheme with emailId["+emailId + "], attachmentName["+a.getAttachmentName()+"]");
			    return new Values(emailId, a.getAttachmentName(), a.getContent());
		    }else{
		    	
		    	LOG.error("message is not an instance of EmailAttachment.");
		    	return null;
		    }		
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

	public Fields getOutputFields() {
		return new Fields("emailId", "attachmentName", "attachmentContent");
		
	}

}
	