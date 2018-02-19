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
public class Perception {
    private String mtvo;
    private String tienda;
    private String numeroRecibo;
    private String numeroOrden;
    private String numeroPedidoAdicional;
    private String numeroRemision;
    private String fecha;
    private String valor;
    private String iva;
    private String neto;
    private Validation validacion;
    
    public Perception()
    {
        this.mtvo = "";
        this.tienda = "";
        this.numeroRecibo = "";
        this.numeroOrden = "";
        this.numeroPedidoAdicional = "";
        this.numeroRemision = "";
        this.fecha = "";
        this.valor = "";
        this.iva = "";
        this.neto = "";
        this.validacion = new Validation();
    }
    
    public Perception(String mtvo, String tienda, String numeroRecibo, String numeroOrden, String numeroPedidoAdicional, String numeroRemision, String fecha, String valor, String iva, String neto, Validation validacion)
    {
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
        this.validacion = validacion;
    }
    
    public Perception(Perception percepcion)
    {
        this.mtvo = percepcion.getMtvo();
        this.tienda = percepcion.getTienda();
        this.numeroRecibo = percepcion.getNumeroRecibo();
        this.numeroOrden = percepcion.getNumeroOrden();
        this.numeroPedidoAdicional = percepcion.getNumeroPedidoAdicional();
        this.numeroRemision = percepcion.getNumeroRemision();
        this.fecha = percepcion.getFecha();
        this.valor = percepcion.getValor();
        this.iva = percepcion.getIva();
        this.neto = percepcion.getNeto();
        this.validacion = percepcion.getValidacion();
    }
    
    public String getMtvo() {
        return mtvo;
    }

    public void setMtvo(String mtvo) {
        this.mtvo = mtvo;
    }

    public String getTienda() {
        return tienda;
    }

    public void setTienda(String tienda) {
        this.tienda = tienda;
    }

    public String getNumeroRecibo() {
        return numeroRecibo;
    }

    public void setNumeroRecibo(String numeroRecibo) {
        this.numeroRecibo = numeroRecibo;
    }

    public String getNumeroOrden() {
        return numeroOrden;
    }

    public void setNumeroOrden(String numeroOrden) {
        this.numeroOrden = numeroOrden;
    }

    public String getNumeroPedidoAdicional() {
        return numeroPedidoAdicional;
    }

    public void setNumeroPedidoAdicional(String numeroPedidoAdicional) {
        this.numeroPedidoAdicional = numeroPedidoAdicional;
    }

    public String getNumeroRemision() {
        return numeroRemision;
    }

    public void setNumeroRemision(String numeroRemision) {
        this.numeroRemision = numeroRemision;
    }

    public String getFecha() {
        return fecha;
    }

    public void setFecha(String fecha) {
        this.fecha = fecha;
    }

    public String getValor() {
        return valor;
    }

    public void setValor(String valor) {
        this.valor = valor;
    }

    public String getIva() {
        return iva;
    }

    public void setIva(String iva) {
        this.iva = iva;
    }

    public String getNeto() {
        return neto;
    }

    public void setNeto(String neto) {
        this.neto = neto;
    }

    public Validation getValidacion() {
        return validacion;
    }

    public void setValidacion(Validation validacion) {
        this.validacion = validacion;
    }
}
