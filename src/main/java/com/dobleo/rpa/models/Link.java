/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.models;

/**
 *
 * @author X220
 */
public class Link {
    private int id;
    private String abreviacionVenta;
    private String diferencia;
    private String porcentaje;
    private String busqueda;
    private Sale venta;
    private Reception recepcion;
    private Document documento;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAbreviacionVenta() {
        return abreviacionVenta;
    }

    public void setAbreviacionVenta(String abreviacionVenta) {
        this.abreviacionVenta = abreviacionVenta;
    }

    public String getDiferencia() {
        return diferencia;
    }

    public void setDiferencia(String diferencia) {
        this.diferencia = diferencia;
    }

    public String getPorcentaje() {
        return porcentaje;
    }

    public void setPorcentaje(String porcentaje) {
        this.porcentaje = porcentaje;
    }

    public String getBusqueda() {
        return busqueda;
    }

    public void setBusqueda(String busqueda) {
        this.busqueda = busqueda;
    }

    public Sale getVenta() {
        return venta;
    }

    public void setVenta(Sale venta) {
        this.venta = venta;
    }

    public Reception getRecepcion() {
        return recepcion;
    }

    public void setRecepcion(Reception recepcion) {
        this.recepcion = recepcion;
    }

    public Document getDocumento() {
        return documento;
    }

    public void setDocumento(Document documento) {
        this.documento = documento;
    }
}
