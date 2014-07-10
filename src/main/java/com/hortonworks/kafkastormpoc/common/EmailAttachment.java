package com.hortonworks.kafkastormpoc.common;

import java.io.Serializable;

public class EmailAttachment implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 8656133860943077949L;

	int emailId;
    String attachmentName;
    byte[] attachmentContent;
    
    public EmailAttachment(int id, String name, byte[] content){
    	this.emailId=id;
    	this.attachmentName=name;
    	this.attachmentContent = content;
    }
    
    public int getEmailId(){
    	return this.emailId;
    }
    public String getAttachmentName(){
    	return this.attachmentName;
    }
    public byte[] getContent(){
    	return this.attachmentContent;
    }
}
