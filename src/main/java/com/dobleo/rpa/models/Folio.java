/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.models;

import com.dobleo.rpa.email.EmailMessage;
import java.util.Date;

/**
 *
 * @author X220
 */
public class Folio {
    private boolean estado;
    private int tipo;
    private Date desde;
    private Date hasta;
    private EmailMessage mensaje;

    public boolean isEstado() {
        return estado;
    }

    public void setEstado(boolean estado) {
        this.estado = estado;
    }

    public int getTipo() {
        return tipo;
    }

    public void setTipo(int tipo) {
        this.tipo = tipo;
    }

    public Date getDesde() {
        return desde;
    }

    public void setDesde(Date desde) {
        this.desde = desde;
    }

    public Date getHasta() {
        return hasta;
    }

    public void setHasta(Date hasta) {
        this.hasta = hasta;
    }
    
    

    public EmailMessage getMensaje() {
        return mensaje;
    }

    public void setMensaje(EmailMessage mensaje) {
        this.mensaje = mensaje;
    }
    
    
}
