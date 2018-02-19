/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.email;

/**
 *
 * @author X220
 */
public class EmailMessage {
    private long uid;
    private boolean processed;
    private String subject;
    private String message;
    
    public EmailMessage()
    {
        this.uid = 0;
        this.processed = false;
        this.subject = ""; 
        this.message = ""; 
    }
    
    public EmailMessage(long uid, boolean processed, String message)
    {
        this.uid = uid;
        this.processed = processed;
        this.subject = ""; 
        this.message = message;
    }
    
    public EmailMessage(EmailMessage emailMessage)
    {
        this.uid = emailMessage.getUid();
        this.processed = emailMessage.isProcessed();
        this.subject = emailMessage.getSubject();
        this.message = emailMessage.getMessage();
    }

    public long getUid() {
        return uid;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }
    
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
