/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.file;

/**
 *
 * @author X220
 */
public class Validation {
    private int id;
    private boolean mtvo;
    private boolean tienda;
    private boolean numeroRecibo;
    private boolean numeroOrden;
    private boolean numeroPedidoAdicional;
    private boolean numeroRemision;
    private boolean fecha;
    private boolean valor;
    private boolean iva;
    private boolean neto;
    
    public Validation()
    {
        this.id = 0;
        this.mtvo = false;
        this.tienda = false;
        this.numeroRecibo = false;
        this.numeroOrden = false;
        this.numeroPedidoAdicional = false;
        this.numeroRemision = false;
        this.fecha = false;
        this.valor = false;
        this.iva = false;
        this.neto = false;
    }
    
    public Validation(int id, boolean mtvo, boolean tienda, boolean numeroRecibo, boolean numeroOrden, boolean numeroPedidoAdicional, boolean numeroRemision, boolean fecha, boolean valor, boolean iva, boolean neto)
    {
        this.id = id;
        this.mtvo = mtvo;
        this.tienda = tienda;
        this.numeroRecibo = numeroRecibo;
        this.numeroOrden = numeroOrden;
        this.numeroPedidoAdicional = numeroPedidoAdicional;
        this.numeroRemision = numeroRemision;
        this.fecha = fecha;
        this.valor = valor;
        this.iva = iva;
        this.neto = neto;
    }
    
    public Validation(Validation validacion)
    {
        this.id = validacion.getId();
        this.mtvo = validacion.isFecha();
        this.tienda = validacion.isTienda();
        this.numeroRecibo = validacion.isNumeroRecibo();
        this.numeroOrden = validacion.isNumeroOrden();
        this.numeroPedidoAdicional = validacion.isNumeroPedidoAdicional();
        this.numeroRemision = validacion.isNumeroRemision();
        this.fecha = validacion.isFecha();
        this.valor = validacion.isValor();
        this.iva = validacion.isIva();
        this.neto = validacion.isNeto();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
    
    

    public boolean isMtvo() {
        return mtvo;
    }

    public void setMtvo(boolean mtvo) {
        this.mtvo = mtvo;
    }

    public boolean isTienda() {
        return tienda;
    }

    public void setTienda(boolean tienda) {
        this.tienda = tienda;
    }

    public boolean isNumeroRecibo() {
        return numeroRecibo;
    }

    public void setNumeroRecibo(boolean numeroRecibo) {
        this.numeroRecibo = numeroRecibo;
    }

    public boolean isNumeroOrden() {
        return numeroOrden;
    }

    public void setNumeroOrden(boolean numeroOrden) {
        this.numeroOrden = numeroOrden;
    }

    public boolean isNumeroPedidoAdicional() {
        return numeroPedidoAdicional;
    }

    public void setNumeroPedidoAdicional(boolean numeroPedidoAdicional) {
        this.numeroPedidoAdicional = numeroPedidoAdicional;
    }

    public boolean isNumeroRemision() {
        return numeroRemision;
    }

    public void setNumeroRemision(boolean numeroRemision) {
        this.numeroRemision = numeroRemision;
    }

    public boolean isFecha() {
        return fecha;
    }

    public void setFecha(boolean fecha) {
        this.fecha = fecha;
    }

    public boolean isValor() {
        return valor;
    }

    public void setValor(boolean valor) {
        this.valor = valor;
    }

    public boolean isIva() {
        return iva;
    }

    public void setIva(boolean iva) {
        this.iva = iva;
    }

    public boolean isNeto() {
        return neto;
    }

    public void setNeto(boolean neto) {
        this.neto = neto;
    }
}
