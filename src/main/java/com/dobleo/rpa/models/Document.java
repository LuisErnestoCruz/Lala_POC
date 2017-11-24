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
public class Document {
    private int id;
    private String numeroFolio;
    private String nombre;
    private String porcentajeIncidencia;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNumeroFolio() {
        return numeroFolio;
    }

    public void setNumeroFolio(String numeroFolio) {
        this.numeroFolio = numeroFolio;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }
    
    public String getPorcentajeIncidencia() {
        return porcentajeIncidencia;
    }

    public void setPorcentajeIncidencia(String porcentajeIncidencia) {
        this.porcentajeIncidencia = porcentajeIncidencia;
    }
}
