/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.file;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author X220
 */
public class Rule {
    private Perception perception;
    private Validation validation;
    private ArrayList<Perception> listPerception; 
    private ArrayList<Perception> listInvalidPerception;
    

    public Perception getPerception() {
        return perception;
    }

    public void setPerception(Perception perception) {
        this.perception = perception;
    }

    public Validation getValidation() {
        return validation;
    }

    public void setValidation(Validation validation) {
        this.validation = validation;
    }

    public ArrayList<Perception> getListPerception() {
        return listPerception;
    }

    public void setListPerception(ArrayList<Perception> listPerception) {
        this.listPerception = listPerception;
    }

    public ArrayList<Perception> getListInvalidPerception() {
        return listInvalidPerception;
    }

    public void setListInvalidPerception(ArrayList<Perception> listInvalidPerception) {
        this.listInvalidPerception = listInvalidPerception;
    }
    
    public Perception oneRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isMtvo() == false)
                    {
                        if(StringUtils.isNotBlank(perception.getValor()) && StringUtils.isNotEmpty(perception.getValor()) && StringUtils.isNotBlank(perception.getIva()) && StringUtils.isNotEmpty(perception.getIva()) && StringUtils.isNotBlank(perception.getFecha()) && StringUtils.isNotEmpty(perception.getFecha()))
                        {
                            if(perception.getFecha().indexOf("-") >= 0 && perception.getIva().indexOf("-") >= 0)
                            {
                                nuevaPercepcion.setMtvo("DEVOLUCIONES");
                                nuevaPercepcion.setTienda(perception.getMtvo());
                                nuevaPercepcion.setNumeroRecibo(perception.getTienda());
                                nuevaPercepcion.setNumeroOrden(perception.getNumeroRecibo());
                                nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroOrden());
                                nuevaPercepcion.setNumeroRemision(perception.getNumeroPedidoAdicional());
                                nuevaPercepcion.setFecha(perception.getNumeroRemision());
                                nuevaPercepcion.setValor(perception.getFecha());
                                nuevaPercepcion.setIva(perception.getValor());
                                nuevaPercepcion.setNeto(perception.getIva());
                            }
                            else
                            {
                                nuevaPercepcion.setMtvo("RECEPCIONES");
                                nuevaPercepcion.setTienda(perception.getMtvo());
                                nuevaPercepcion.setNumeroRecibo(perception.getTienda());
                                nuevaPercepcion.setNumeroOrden(perception.getNumeroRecibo());
                                nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroOrden());
                                nuevaPercepcion.setNumeroRemision(perception.getNumeroPedidoAdicional());
                                nuevaPercepcion.setFecha(perception.getNumeroRemision());
                                nuevaPercepcion.setValor(perception.getFecha());
                                nuevaPercepcion.setIva(perception.getValor());
                                nuevaPercepcion.setNeto(perception.getIva());
                            }
                            
                            
                        }
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
        
        
        return nuevaPercepcion;
    }
    
    public Perception twoRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isTienda() == false)
                    {
                        
                        nuevaPercepcion.setMtvo(perception.getMtvo());
                        nuevaPercepcion.setTienda("#N/A");
                        nuevaPercepcion.setNumeroRecibo(perception.getTienda());
                        nuevaPercepcion.setNumeroOrden(perception.getNumeroRecibo());
                        nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroOrden());
                        nuevaPercepcion.setNumeroRemision(perception.getNumeroPedidoAdicional());
                        nuevaPercepcion.setFecha(perception.getNumeroRemision());
                        nuevaPercepcion.setValor(perception.getFecha());
                        nuevaPercepcion.setIva(perception.getValor());
                        nuevaPercepcion.setNeto(perception.getIva());
                        
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
        
        
        return nuevaPercepcion;
    }
    
    public Perception threeRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isNumeroRecibo() == false)
                    {
                        if(perception.getMtvo().equals("RECEPCIONES"))
                        {
                            nuevaPercepcion.setMtvo(perception.getMtvo());
                            nuevaPercepcion.setTienda(perception.getTienda());
                            nuevaPercepcion.setNumeroRecibo("");
                            nuevaPercepcion.setNumeroOrden(perception.getNumeroRecibo());
                            nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroOrden());
                            nuevaPercepcion.setNumeroRemision(perception.getNumeroPedidoAdicional());
                            nuevaPercepcion.setFecha(perception.getNumeroRemision());
                            nuevaPercepcion.setValor(perception.getFecha());
                            nuevaPercepcion.setIva(perception.getValor());
                            nuevaPercepcion.setNeto(perception.getIva());
                        }
                        else 
                        {
                            nuevaPercepcion = perception;
                        }
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
        
        return nuevaPercepcion;
    }
    
    public Perception fourRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isNumeroOrden() == false)
                    {
                        nuevaPercepcion.setMtvo(perception.getMtvo());
                        nuevaPercepcion.setTienda(perception.getTienda());
                        nuevaPercepcion.setNumeroRecibo(perception.getNumeroRecibo());
                        nuevaPercepcion.setNumeroOrden("");
                        nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroOrden());
                        nuevaPercepcion.setNumeroRemision(perception.getNumeroPedidoAdicional());
                        nuevaPercepcion.setFecha(perception.getNumeroRemision());
                        nuevaPercepcion.setValor(perception.getFecha());
                        nuevaPercepcion.setIva(perception.getValor());
                        nuevaPercepcion.setNeto(perception.getIva());
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
        
        return nuevaPercepcion;
    }
    
    //Este puede ser no necesario
    public Perception fiveRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isNumeroPedidoAdicional() == false)
                    {
                        nuevaPercepcion.setMtvo(perception.getMtvo());
                        nuevaPercepcion.setTienda(perception.getTienda());
                        nuevaPercepcion.setNumeroRecibo(perception.getNumeroRecibo());
                        nuevaPercepcion.setNumeroOrden(perception.getNumeroOrden());
                        nuevaPercepcion.setNumeroPedidoAdicional("");
                        nuevaPercepcion.setNumeroRemision("");
                        nuevaPercepcion.setFecha(perception.getNumeroPedidoAdicional());
                        nuevaPercepcion.setValor(perception.getNumeroRemision());
                        nuevaPercepcion.setIva(perception.getFecha());
                        nuevaPercepcion.setNeto(perception.getValor());
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
        
        return nuevaPercepcion;
    }
    
    public Perception sixRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isNumeroRemision() == false)
                    {
                        nuevaPercepcion.setMtvo(perception.getMtvo());
                        nuevaPercepcion.setTienda(perception.getTienda());
                        nuevaPercepcion.setNumeroRecibo(perception.getNumeroRecibo());
                        nuevaPercepcion.setNumeroOrden(perception.getNumeroOrden());
                        nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroPedidoAdicional());
                        nuevaPercepcion.setNumeroRemision("");
                        nuevaPercepcion.setFecha(perception.getNumeroRemision());
                        nuevaPercepcion.setValor(perception.getFecha());
                        nuevaPercepcion.setIva(perception.getValor());
                        nuevaPercepcion.setNeto(perception.getIva());
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
     
        return nuevaPercepcion;
    }
    
    public Perception sevenRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isFecha() == false)
                    {
                        nuevaPercepcion.setMtvo(perception.getMtvo());
                        nuevaPercepcion.setTienda(perception.getTienda());
                        nuevaPercepcion.setNumeroRecibo(perception.getNumeroRecibo());
                        nuevaPercepcion.setNumeroOrden(perception.getNumeroOrden());
                        nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroPedidoAdicional());
                        nuevaPercepcion.setNumeroRemision(perception.getNumeroRemision());
                        nuevaPercepcion.setFecha(perception.getValor());
                        nuevaPercepcion.setValor(perception.getIva());
                        nuevaPercepcion.setIva(perception.getNeto());
                        nuevaPercepcion.setNeto(perception.getIva());
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
        
        return nuevaPercepcion;
    }
    
    public Perception eightRule(Perception perception)
    {
        Perception nuevaPercepcion = new Perception();
        try
        {
            nuevaPercepcion = new Perception();
            if(perception != null)
            {
                if(perception.getValidacion() != null)
                {
                    if(perception.getValidacion().isValor() == false)
                    {
                        nuevaPercepcion.setMtvo(perception.getMtvo());
                        nuevaPercepcion.setTienda(perception.getTienda());
                        nuevaPercepcion.setNumeroRecibo(perception.getNumeroRecibo());
                        nuevaPercepcion.setNumeroOrden(perception.getNumeroOrden());
                        nuevaPercepcion.setNumeroPedidoAdicional(perception.getNumeroPedidoAdicional());
                        nuevaPercepcion.setNumeroRemision(perception.getNumeroRemision());
                        nuevaPercepcion.setFecha(perception.getFecha());
                        nuevaPercepcion.setValor(perception.getIva());
                        nuevaPercepcion.setIva(perception.getValor());
                        nuevaPercepcion.setNeto(perception.getIva());
                    }
                }
            }
        }
        catch(Exception er)
        {
            
        }
        
        return nuevaPercepcion;
    }
    
    public void asignRule(ArrayList<Perception> listPerceptionIssue)
    {
        Perception newPerception = null;
        Validation newValidation = null;
        if(listPerceptionIssue != null)
        {
            if(listPerceptionIssue.size() > 0)
            {
                this.listPerception = new ArrayList<Perception>();
                for(Perception percecption: listPerceptionIssue)
                {
                    if(percecption.getValidacion() != null)
                    {
                        if(!percecption.getValidacion().isMtvo())
                        {
                            newPerception = oneRule(percecption);
                            
                            this.listPerception.add(newPerception);
                        }
                        else if(!percecption.getValidacion().isTienda())
                        {
                            newPerception = twoRule(percecption); 
                            this.listPerception.add(newPerception);
                            
                        }
                        else if(!percecption.getValidacion().isNumeroRecibo())
                        { 
                            newPerception = threeRule(percecption); 
                            this.listPerception.add(newPerception);
                        }
                        else if(!percecption.getValidacion().isNumeroOrden())
                        {
                            newPerception = fourRule(percecption); 
                            this.listPerception.add(newPerception);
                        }
                        else if(!percecption.getValidacion().isNumeroPedidoAdicional())
                        {
                            newPerception = fiveRule(percecption); 
                            this.listPerception.add(newPerception);
                        }
                        else if(!percecption.getValidacion().isNumeroRemision())
                        {
                            newPerception = sixRule(percecption); 
                            this.listPerception.add(newPerception);
                        }
                        else if(!percecption.getValidacion().isFecha())
                        {
                            newPerception = sevenRule(percecption); 
                            this.listPerception.add(newPerception);
                        }
                        else if(!percecption.getValidacion().isValor())
                        {
                           
                            newPerception = eightRule(percecption); 
                            this.listPerception.add(newPerception);
                        }
                    }
                    
                    
                }
                
                if(this.listPerception != null)
                {
                    if(this.listPerception.size() > 0)
                    {
                        for(Perception perception: listPerception)
                        {
                            newValidation = new Validation();
                            for(int d = 1; d <= 10; d++)
                            {
                                switch(d)
                                {
                                    case 1: newValidation.setMtvo(checkRule(1, perception.getMtvo())); break;
                                    case 2: newValidation.setTienda(checkRule(2, perception.getTienda())); break;
                                    case 3: newValidation.setNumeroRecibo(checkRule(3, perception.getNumeroRecibo())); break;
                                    case 4: newValidation.setNumeroOrden(checkRule(4, perception.getNumeroOrden())); break;
                                    case 5: newValidation.setNumeroPedidoAdicional(checkRule(5, perception.getNumeroPedidoAdicional())); break;
                                    case 6: newValidation.setNumeroRemision(checkRule(6, perception.getNumeroRemision())); break;
                                    case 7: newValidation.setFecha(checkRule(7, perception.getFecha())); break;
                                    case 8: newValidation.setValor(checkRule(8, perception.getValor())); break;
                                    case 9: newValidation.setIva(checkRule(9, perception.getIva())); break;
                                    case 10: newValidation.setNeto(checkRule(10, perception.getNeto()));  break;
                                }
                            }
                            perception.setValidacion(newValidation);
                        }
                    }
                }
            }
        }
    }
    
    public boolean checkRule(int position, String text)
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
    
    public void orderInvalidPerceptions(ArrayList<Perception> listPerceptions)
    {
        try
        {
            ArrayList<Perception> replaceInvalidPerceptions = null;
            ArrayList<Perception> receptionsIssue = null;
            ArrayList<Perception> replaceReceptionsIssue = null;
            ArrayList<Perception> refundsIssue = null;
            if(listPerceptions != null)
            {
                if(listPerceptions.size() > 0)
                {

                    if(this.listPerception != null)
                    {
                        if(this.listPerception.size() > 0)
                        {
                            replaceInvalidPerceptions = new ArrayList<Perception>(); 
                            for(Perception perceptionIssue: this.listPerception)
                            {
                                validation = new Validation();
                                for(int a = 1; a <= 10; a++)
                                {
                                    switch(a)
                                    {
                                        case 1: validation.setMtvo(checkRule(1, perceptionIssue.getMtvo())); break;
                                        case 2: validation.setTienda(checkRule(2, perceptionIssue.getTienda())); break;
                                        case 3: validation.setNumeroRecibo(checkRule(3, perceptionIssue.getNumeroRecibo())); break;
                                        case 4: validation.setNumeroOrden(checkRule(4, perceptionIssue.getNumeroOrden())); break;
                                        case 5: validation.setNumeroPedidoAdicional(checkRule(5, perceptionIssue.getNumeroPedidoAdicional())); break;
                                        case 6: validation.setNumeroRemision(checkRule(6, perceptionIssue.getNumeroRemision())); break;
                                        case 7: validation.setFecha(checkRule(7, perceptionIssue.getFecha())); break;
                                        case 8: validation.setValor(checkRule(8, perceptionIssue.getValor())); break;
                                        case 9: validation.setIva(checkRule(9, perceptionIssue.getIva())); break;
                                        case 10: validation.setNeto(checkRule(10, perceptionIssue.getNeto()));  break;
                                    }
                                }

                                perceptionIssue.setValidacion(validation);
                                replaceInvalidPerceptions.add(perceptionIssue);
                            }

                            if(replaceInvalidPerceptions != null)
                            {
                                if(replaceInvalidPerceptions.size() > 0)
                                {
                                    receptionsIssue = new ArrayList<Perception>();
                                    refundsIssue = new ArrayList<Perception>(); 
                                    for(Perception replaceInvalidPerception: replaceInvalidPerceptions)
                                    {
                                        if(replaceInvalidPerception.getMtvo().equals("RECEPCIONES"))
                                        {
                                            receptionsIssue.add(replaceInvalidPerception);
                                        }
                                        else if(replaceInvalidPerception.getMtvo().equals("DEVOLUCIONES"))
                                        {
                                            refundsIssue.add(replaceInvalidPerception);
                                        }
                                    }

                                    if(receptionsIssue != null)
                                    {
                                        if(receptionsIssue.size() > 0)
                                        {
                                            replaceReceptionsIssue = new ArrayList<Perception>(); 
                                            for(Perception receptionIssue: receptionsIssue)
                                            {
                                                validation = new Validation();
                                                if(receptionIssue.getValidacion() != null)
                                                {
                                                    validation = receptionIssue.getValidacion();
                                                    if(validation.isMtvo() && validation.isTienda() && validation.isNumeroRecibo() && !validation.isNumeroOrden() && validation.isNumeroPedidoAdicional() && validation.isNumeroRemision() && validation.isFecha() && validation.isValor() && validation.isIva() && validation.isNeto())
                                                    {
                                                        perception = new Perception();
                                                        perception.setMtvo(receptionIssue.getMtvo());
                                                        perception.setTienda(receptionIssue.getTienda());
                                                        perception.setNumeroRecibo("");
                                                        perception.setNumeroOrden(receptionIssue.getNumeroRecibo());
                                                        perception.setNumeroPedidoAdicional(receptionIssue.getNumeroPedidoAdicional());
                                                        perception.setNumeroRemision(receptionIssue.getNumeroRemision());
                                                        perception.setFecha(receptionIssue.getFecha());
                                                        perception.setValor(receptionIssue.getValor());
                                                        perception.setIva(receptionIssue.getIva());
                                                        perception.setNeto(receptionIssue.getNeto());
                                                        replaceReceptionsIssue.add(perception); 
                                                    }
                                                }
                                            }
                                            
                                            if(replaceReceptionsIssue != null)
                                            {
                                                if(replaceReceptionsIssue.size() > 0)
                                                {
                                                    for(Perception replaceReceptionIssue: replaceReceptionsIssue)
                                                    {
                                                        validation = new Validation();
                                                        for(int b = 1; b <= 10; b++)
                                                        {
                                                            switch(b)
                                                            {
                                                                case 1: validation.setMtvo(checkRule(1, replaceReceptionIssue.getMtvo())); break;
                                                                case 2: validation.setTienda(checkRule(2, replaceReceptionIssue.getTienda())); break;
                                                                case 3: validation.setNumeroRecibo(checkRule(3, replaceReceptionIssue.getNumeroRecibo())); break;
                                                                case 4: validation.setNumeroOrden(checkRule(4, replaceReceptionIssue.getNumeroOrden())); break;
                                                                case 5: validation.setNumeroPedidoAdicional(checkRule(5, replaceReceptionIssue.getNumeroPedidoAdicional())); break;
                                                                case 6: validation.setNumeroRemision(checkRule(6, replaceReceptionIssue.getNumeroRemision())); break;
                                                                case 7: validation.setFecha(checkRule(7, replaceReceptionIssue.getFecha())); break;
                                                                case 8: validation.setValor(checkRule(8, replaceReceptionIssue.getValor())); break;
                                                                case 9: validation.setIva(checkRule(9, replaceReceptionIssue.getIva())); break;
                                                                case 10: validation.setNeto(checkRule(10, replaceReceptionIssue.getNeto()));  break;
                                                            }
                                                        }
                                                        
                                                        replaceReceptionIssue.setValidacion(validation);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                        }
                    }
                }
            }
        }
        catch(Exception e)
        {
            
        }
    }
}
