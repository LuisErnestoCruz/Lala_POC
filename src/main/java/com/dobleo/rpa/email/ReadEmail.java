/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.email;

import com.dobleo.rpa.file.Header;
import com.dobleo.rpa.file.Perception;
import com.dobleo.rpa.file.Validation;
import com.novayre.jidoka.client.api.IJidokaServer;
import com.sun.mail.gimap.GmailFolder;
import com.sun.mail.gimap.GmailRawSearchTerm;
import com.sun.mail.gimap.GmailStore;
import com.sun.mail.imap.IMAPFolder;
import com.sun.mail.imap.IMAPStore;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Properties;
import javax.mail.BodyPart;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMultipart;
import javax.mail.search.AndTerm;
import javax.mail.search.FlagTerm;
import javax.mail.search.FromTerm;
import javax.mail.search.SearchTerm;
import javax.mail.search.SubjectTerm;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;

/**
 *
 * @author X220
 */
public class ReadEmail {
    private String username;
    private String password;
    private String from;
    private String subject;
    private IJidokaServer<?> server;
    private ArrayList<String> columnNames;
    private ArrayList<Header> headers;
    private ArrayList<Perception> perceptions;
    
    public ReadEmail()
    {
       this.username = ""; 
       this.password = "";
       this.from = ""; 
       this.subject = ""; 
       this.columnNames = null;
       this.headers = null;
       this.perceptions = null;
       this.server = null;
    }
    
    public ReadEmail(String username, String password, String from, String subject, ArrayList<String> columnNames, ArrayList<Header> headers, ArrayList<Perception> perceptions)
    {
        this.username = username; 
        this.password = password;
        this.from = from;
        this.subject = subject;
        this.columnNames = columnNames;
        this.headers = headers;
        this.perceptions = perceptions;
        this.server = null;
    }
    
    public ReadEmail(ReadEmail readEmail)
    {
        this.username = readEmail.getUsername();
        this.password = readEmail.getPassword();
        this.from = readEmail.getFrom();
        this.subject = readEmail.getSubject();
        this.columnNames = readEmail.getColumnNames();
        this.headers = readEmail.getHeaders();
        this.perceptions = readEmail.getPerceptions();
        this.server = readEmail.getServer();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }
    
    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(ArrayList<String> columnNames) {
        this.columnNames = columnNames;
    }

    public ArrayList<Header> getHeaders() {
        return headers;
    }

    public void setHeaders(ArrayList<Header> headers) {
        this.headers = headers;
    }

    public ArrayList<Perception> getPerceptions() {
        return perceptions;
    }

    public void setPerceptions(ArrayList<Perception> perceptions) {
        this.perceptions = perceptions;
    }
    
    public IJidokaServer<?> getServer() {
        return server;
    }

    public void setServer(IJidokaServer<?> server) {
        this.server = server;
    }
    
    public Message[] gmailReadUnseenEmails()
    {
        Message[] messages = null;
        try
        {
            if(from != null && subject != null)
            {
                if(StringUtils.isNotBlank(from) && StringUtils.isNotEmpty(from) && StringUtils.isNotBlank(subject) && StringUtils.isNotEmpty(subject))
                {
                    StringBuilder rawSearchTerm = new StringBuilder();
                    Properties gmailProperties = new Properties();
                    gmailProperties.setProperty("mail.store.protocol", "gimap");

                    Session gmailSession = Session.getDefaultInstance(gmailProperties, null);
                    gmailSession.setDebug(true);
                    GmailStore gmailStore = (GmailStore) gmailSession.getStore("gimap");
                    gmailStore.connect("imap.gmail.com", username, password);

                    GmailFolder gmailInboxFolder = (GmailFolder) gmailStore.getFolder("INBOX");
                    gmailInboxFolder.open(Folder.READ_ONLY);
                    
                    rawSearchTerm.append("is:unread");
                    rawSearchTerm.append(" ");
                    rawSearchTerm.append("from:(");
                    rawSearchTerm.append(from);
                    rawSearchTerm.append(")");
                    rawSearchTerm.append(" ");
                    rawSearchTerm.append("subject:(");
                    rawSearchTerm.append(subject);
                    rawSearchTerm.append(")"); 
                    GmailRawSearchTerm gmailRawSearchTerm = new GmailRawSearchTerm(rawSearchTerm.toString());

                    messages = gmailInboxFolder.search(gmailRawSearchTerm);
                    
                    gmailInboxFolder.close(false);
                    gmailStore.close();
                }
            }
        }
        catch (NoSuchProviderException e) {
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
            //System.exit(1);
        } catch (MessagingException e) {
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
            //System.exit(2);
        }
         catch (Exception e)
         {
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
         }
        return messages; 
    }
    
    public ArrayList<EmailMessage> readUnseenEmails()
    {
        ArrayList<EmailMessage> emailMessages = null;
        try
        {
            String messageText = null;
            Message messages[] = null;
            EmailMessage emailMessage = null;
            Properties emailProperties = new Properties();        
            emailProperties.setProperty("mail.store.protocol", "imap");
            emailProperties.setProperty("mail.imap.ssl.enable", "true"); 
            Session mailSession = Session.getInstance(emailProperties); 
            mailSession.setDebug(true);
            IMAPStore mailStore = (IMAPStore) mailSession.getStore("imap");
            mailStore.connect("p3plcpnl0484.prod.phx3.secureserver.net", username, password);
            
            IMAPFolder inboxFolder = (IMAPFolder) mailStore.getFolder("INBOX");
            inboxFolder.open(IMAPFolder.READ_ONLY);
            
            Flags seen = new Flags(Flags.Flag.SEEN); 
            FlagTerm unseenFlagTerm = new FlagTerm(seen, false); 
            FromTerm fromTerm = new FromTerm(new InternetAddress(from));
            SubjectTerm subjectTerm = new SubjectTerm(subject);
            
            SearchTerm searchTerm = fromTerm; 
            SearchTerm searchTermSecond = new AndTerm(unseenFlagTerm, subjectTerm);
            SearchTerm finalSearchTerm = new AndTerm(searchTerm, searchTermSecond);
            
            messages = inboxFolder.search(finalSearchTerm);
            
            if(messages != null)
            {
                if(messages.length > 0)
                {
                    emailMessages = new ArrayList<EmailMessage>();
                    for(Message message: messages)
                    {
                        emailMessage = new EmailMessage();
                        emailMessage.setUid(inboxFolder.getUID(message));
                        emailMessage.setProcessed(false);
                        emailMessage.setSubject(message.getSubject());
                        messageText = getTextFromMessage(message);
                        /*messageText = messageText.replace(")", "");
                        messageText = messageText.replace("class=\"gmail-", "class=\"");
                        messageText = messageText.replace("class=D", "class=");*/
                        emailMessage.setMessage(messageText);
                        emailMessages.add(emailMessage);
                    }
                }
            }
            
            inboxFolder.close(false);
            mailStore.close();
        }
        catch(NoSuchProviderException e)
        {
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        catch(MessagingException e)
        {
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        catch(Exception e)
        {
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return emailMessages; 
    }
    
    public String formatInformationFromEmailContent(EmailMessage emailMessage)
    {
        String fileName = ""; 
        try
        {
            boolean checkFolioName = false;
            boolean isPerception = false;
            boolean invalidPerception = false;
            int idValidation = 0;
            int countColumn = 0;
            int countHeader = 0;
            int currentNumberColumn = 0;
            String tableElement = "";
            Header header = new Header();
            Perception perception = new Perception();
            Validation validation = new Validation();
            Document document = null;
            ArrayList<String> listColumnNames = new ArrayList<String>(); 
            ArrayList<String> listCurrentColumnNames = new ArrayList<String>();
            ArrayList<Header> listHeaderContent = new ArrayList<Header>();
            ArrayList<Perception> listPerception = new ArrayList<Perception>();
            ArrayList<Perception> listInvalidPerception = new ArrayList<Perception>();
            LinkedHashSet<String> listOrderColumnNames = null;
            
            if(emailMessage != null)
            {
                //Convierto el mensaje en un documento HTML
                document = Jsoup.parse(emailMessage.getMessage());
                //Despues de convertir la cadena a documento despues obtengo el elemento de tipo table en el documento
                Element table = document.select("table").get(0);
                //Obtengo el codigo html solo de lo que hay dentro de la etiqueta table
                tableElement = table.html();
                //Limpio la cadena html para que se pueda leer debido a la inconcistencia de la informacion
                tableElement = tableElement.replace(")", "");
                tableElement = tableElement.replace("class=\"gmail-", "class=\"");
                tableElement = tableElement.replace("class=D", "class=");
                table = Jsoup.parse(tableElement, "", Parser.xmlParser());
                //Realizo una iteracion de todas las filas que tiene la etiqueta table
                for(Element row: table.select("tr"))
                {
                    isPerception = false;
                    invalidPerception = false;
                    countColumn = 1;
                    perception = null;
                    validation = null;
                    perception = new Perception();
                    validation = new Validation();
                    
                    //Itero las columnas que tiene esa fila en particular
                    for(Element column: row.children())
                    {
                        
                        currentNumberColumn = row.children().size();
                        
                        //Si el total de columnas es 3 quiere decir que estoy ubicado el la cabecera de la tabla
                        if(currentNumberColumn == 3)
                        {   
                            if(countHeader == 4)
                            {
                                listHeaderContent.add(header);
                                header = new Header();
                                countHeader = 1;
                            }
                            
                            
                            if(column.className().equals("d"))
                            {
                                if(StringUtils.isNotBlank(column.text()) && StringUtils.isNotEmpty(column.text()))
                                {
                                    switch(countHeader)
                                    {
                                        case 1: header.setColumn1(column.text()); break;
                                        case 2: header.setColumn2(column.text()); break;
                                        case 3: header.setColumn3(column.text()); break;
                                        default: break; 
                                    }
                                }
                            }

                            countHeader++;
                        }
                        
                        //Si se cumple esta condicion quiere decir que estoy en la fila que contiene el nombre de la columnas
                        if(column.className().equals("d2"))
                        {
                            if(StringUtils.isNotBlank(column.text()) && StringUtils.isNotEmpty(column.text()))
                            {
                                System.out.println("Nombre de Columna: " + column.text());
                                listCurrentColumnNames.add(column.text());
                            }
                        }
                        
                        //Si se cumple esta condicion quiere decir que estoy en una fila que contiene la informacion que necesito procesar
                        if(column.className().equals("h"))
                        {
                            //Si se cumple esta condicion quiere decir que la informacion en esta fila contiene la informacion de las 10 columnas
                            if(currentNumberColumn >= 9)
                            {
                                isPerception = true;
                                //System.out.println("Texto de la columna " + numberTimes + ": " + column.text());
                                switch(countColumn)
                                {
                                    case 1: perception.setMtvo(column.text()); validation.setId(idValidation); validation.setMtvo(rulePerception(1, column.text())); if(validation.isMtvo() == false){ invalidPerception = true; } break;
                                    case 2: perception.setTienda(column.text()); validation.setTienda(rulePerception(2, column.text())); if(validation.isTienda() == false){ invalidPerception = true; } break;
                                    case 3: perception.setNumeroRecibo(column.text()); validation.setNumeroRecibo(rulePerception(3, column.text())); if(validation.isNumeroRecibo()== false){ invalidPerception = true; } break;
                                    case 4: perception.setNumeroOrden(column.text()); validation.setNumeroOrden(rulePerception(4, column.text())); if(validation.isNumeroOrden()== false){ invalidPerception = true; } break;
                                    case 5: perception.setNumeroPedidoAdicional(column.text()); validation.setNumeroPedidoAdicional(rulePerception(5, column.text())); if(validation.isNumeroPedidoAdicional()== false){ invalidPerception = true; } break;
                                    case 6: perception.setNumeroRemision(column.text()); validation.setNumeroRemision(rulePerception(6, column.text())); if(validation.isNumeroRemision()== false){ invalidPerception = true; } break;
                                    case 7: perception.setFecha(column.text()); validation.setFecha(rulePerception(7, column.text())); if(validation.isFecha()== false){ invalidPerception = true; } break;
                                    case 8: perception.setValor(column.text()); validation.setValor(rulePerception(8, column.text())); if(validation.isValor()== false){ invalidPerception = true; } break;
                                    case 9: perception.setIva(column.text()); validation.setIva(rulePerception(9, column.text())); if(validation.isIva()== false){ invalidPerception = true; } break;
                                    case 10: perception.setNeto(column.text()); validation.setNeto(rulePerception(10, column.text())); if(validation.isNeto()== false){ invalidPerception = true; } break;
                                }
                                countColumn = countColumn + 1;
                            }
                            
                            //Si se cumple esta condicion quiere decir que en esta fila se hace un calculo mostrando el Total de las filas anteriores a la misma
                            if(currentNumberColumn == 5)
                            {
                                isPerception = true;
                                countColumn = 7;
                                perception.setFecha(column.text());
                            }

                            if(column.text() != null)
                            {
                                //Si se cumple esta condicion lo que se hace es que se llena un objeto de tipo percepcion pero con valores incompletos
                                if(StringUtils.isNotBlank(column.text()) && StringUtils.isNotEmpty(column.text()))
                                {
                                    if(column.text().equals("TOTAL:"))
                                    {
                                        isPerception = true;
                                        countColumn = 7;
                                        perception.setMtvo("");
                                        perception.setFecha(column.text());
                                    }
                                }
                            }
                        }
                        
                        //Si se cumple esta condicion lo que se hace es que seria un objeto de tipo percepcion pero para los totales
                        if(column.className().equals("res1") || column.className().equals("res2"))
                        {
                            countColumn = countColumn + 1;
                            switch(countColumn)
                            {
                                case 8: perception.setValor(column.text()); break;
                                case 9: perception.setIva(column.text()); break;
                                case 10: perception.setNeto(column.text()); break;
                                default: break;
                            }

                        }
                        
                    }
                    
                    //Si se cumple esta condicion entonces el objeto Percepcion se añade a una lista para despues que se pueda escribir en el archivo
                    if(isPerception == true)
                    {
                        perception.setValidacion(validation);
                        listPerception.add(perception);
                        idValidation = listPerception.size(); 
                        //Si se cumple esta condicion entonces el objeto de tipo Percepcion se añade a una lista de Percepciones a tratar como error y corregir el problema
                        if(invalidPerception == true)
                        {
                            listInvalidPerception.add(perception); 
                        }
                    }
                }
                
                //Esta condicion es para cuando el objeto de tipo Header se agrege como la cabecera de la primera hoja del documento se añade a una lista
                if(header != null)
                {
                    listHeaderContent.add(header);
                }
                
                this.perceptions = listPerception;
                this.headers = listHeaderContent;
                //Se eliminan los nombre repetidos y se mantiene el orden de la lista en como fueron insertados
                listOrderColumnNames = new LinkedHashSet<String>(listCurrentColumnNames);
                //Se agregan los nombre ya sin repetir a una nueva lista para que despues sean escritos
                listColumnNames = new ArrayList<String>(listOrderColumnNames);
                this.columnNames = listColumnNames;
                
                countHeader = 0;
                //Bucle que nos ayuda a determinar el nombre final del documento
                for(Header individualHeader: listHeaderContent)
                {
                    for(countHeader = 1; countHeader < 4; countHeader++)
                    {
                        switch(countHeader)
                        {
                            case 1: 
                            if(StringUtils.isNotBlank(individualHeader.getColumn1()) && StringUtils.isNotEmpty(individualHeader.getColumn1()))
                            {
                                if(individualHeader.getColumn1().indexOf(":FP_") >= 0)
                                {
                                    fileName = individualHeader.getColumn1().substring(individualHeader.getColumn1().indexOf(":") + 1, individualHeader.getColumn1().length());
                                    checkFolioName = true;
                                    countHeader = 4;
                                }
                            }
                            break;
                            case 2: 
                            if(StringUtils.isNotBlank(individualHeader.getColumn2()) && StringUtils.isNotEmpty(individualHeader.getColumn2()))
                            {
                                if(individualHeader.getColumn2().indexOf(":FP_") >= 0)
                                {
                                    fileName = individualHeader.getColumn2().substring(individualHeader.getColumn2().indexOf(":") + 1, individualHeader.getColumn2().length());
                                    checkFolioName = true;
                                    countHeader = 4;
                                }
                            }
                            break;
                            case 3: 
                            if(StringUtils.isNotBlank(individualHeader.getColumn3()) && StringUtils.isNotEmpty(individualHeader.getColumn3()))
                            {
                                if(individualHeader.getColumn3().indexOf(":FP_") >= 0)
                                {
                                    fileName = individualHeader.getColumn3().substring(individualHeader.getColumn3().indexOf(":") + 1, individualHeader.getColumn3().length());
                                    checkFolioName = true;
                                    countHeader = 4;
                                }
                            }
                            break;
                        }
                    }
                    
                    if(checkFolioName == true)
                    {
                        break;
                    }
                }
            }
        }
        catch(Exception e)
        {
            fileName = ""; 
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return fileName;
    }
    
    public boolean rulePerception(int position, String text)
    {
        boolean rule = false;
        switch(position)
        {
            case 1: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(text.equals("RECEPCIONES") || text.equals("DEVOLUCIONES"))
                    {
                        rule = true;
                    }
                    else 
                    {
                        rule = false;
                    }
                }
                else
                {
                    rule = false;
                }
                break;
            case 2: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(text.indexOf("-") > 0)
                    {
                        rule = true; 
                    }
                    else
                    {
                        rule = false;
                    }
                }
                else 
                {
                    rule = false;
                }
            break;
            case 3: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(StringUtils.isNumeric(text))
                    {
                        rule = true;
                    }
                    else 
                    {
                        rule = false;
                    }
                }
                else
                {
                    rule = false;
                }
            case 4: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(StringUtils.isNumeric(text))
                    {
                        rule = true;
                    }
                    else
                    {
                        rule = false;
                    }
                }
                else 
                {
                    rule = false;
                }
                break;
            case 5: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(StringUtils.isNumeric(text))
                    {
                        rule = true; 
                    }
                    else
                    {
                        rule = false;
                    }
                }
                else
                {
                    rule = true;
                }
                
                break;
            case 6: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(StringUtils.isNumeric(text))
                    {
                        rule = true; 
                    }
                    else 
                    {
                        rule = false;
                    }
                }
                else 
                {
                    rule = false;
                }
                break;
            case 7: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    try {
                        Format formatter = new SimpleDateFormat("dd-MMM-yy", Locale.US);
                        Date date = (Date) formatter.parseObject(text);
                        //System.out.println(date);
                        rule = true;

                    } catch (Exception e) {
                        //e.printStackTrace();
                        rule = false;
                    }
                }
                else 
                {
                    rule = false; 
                }
                break;
            case 8: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(text.indexOf("$") >= 0)
                    {
                        String chain = text.replace("$", "");
                        chain = chain.replace(",", "");
                        double value = Double.valueOf(chain);
                        if(value != 0)
                        {
                            rule = true;
                        }
                        else 
                        {
                            rule = false;
                        }
                    }
                    else
                    {
                        rule = false;
                    }
                }
                else
                {
                    rule = false;
                }
                break;
            case 9: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text.replace(" ", "");
                    if(text.indexOf("$") >= 0)
                    {
                        String chain = text.replace("$", "");
                        chain = chain.replace(",", "");
                        double value = Double.valueOf(chain);
                        if(value == 0)
                        {
                            rule = true;
                        }
                        else 
                        {
                            rule = false;
                        }
                    }
                    else 
                    {
                        rule = false;
                    }
                }
                else
                {
                    rule = false;
                }
                break;
            case 10: 
                if(StringUtils.isBlank(text) == false && StringUtils.isEmpty(text) == false)
                {
                    text = text.replace(" ", "");
                    if(text.indexOf("$") >= 0)
                    {
                        String chain = text.replace("$", "");
                        chain = chain.replace(",", "");
                        double value = Double.valueOf(chain);
                        if(value != 0)
                        {
                            rule = true;
                        }
                        else 
                        {
                            rule = false;
                        }
                    }
                    else
                    {
                        rule = false;
                    }
                }
                else
                {
                    rule = false;
                }
                break;
            default: break;
        }
        return rule;
    }
    
    private String getTextFromMessage(Message message) throws MessagingException, IOException {
        String result = "";
        if (message.isMimeType("text/plain")) {
            result = message.getContent().toString();
        } else if (message.isMimeType("multipart/*")) {
            MimeMultipart mimeMultipart = (MimeMultipart) message.getContent();
            result = getTextFromMimeMultipart(mimeMultipart);
        }
        return result;
    }
    
    private String getTextFromMimeMultipart(MimeMultipart mimeMultipart)  throws MessagingException, IOException{
        String result = "";
        int count = mimeMultipart.getCount();
        for (int i = 0; i < count; i++) {
            BodyPart bodyPart = mimeMultipart.getBodyPart(i);
            /*if (bodyPart.isMimeType("text/plain")) {
                result = result + "\n" + bodyPart.getContent();
                break; // without break same text appears twice in my tests
            } else*/ if (bodyPart.isMimeType("text/html")) {
                String html = (String) bodyPart.getContent();
                result = html;
                //result = result + "\n" + org.jsoup.Jsoup.parse(html).text();
            } /*else if (bodyPart.getContent() instanceof MimeMultipart){
                result = result + getTextFromMimeMultipart((MimeMultipart)bodyPart.getContent());
            }*/
        }
        return result;
    }
    
}