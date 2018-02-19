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
public class Header {
    private String column1;
    private String column2;
    private String column3;
    
    public Header()
    {
        this.column1 = ""; 
        this.column2 = ""; 
        this.column3 = "";
    }
    
    public Header(String column1, String column2, String column3)
    {
        this.column1 = column1;
        this.column2 = column2;
        this.column3 = column3;
    }
    
    public Header(Header header)
    {
        this.column1 = header.getColumn1();
        this.column2 = header.getColumn2();
        this.column3 = header.getColumn3();
    }
    
    public String getColumn1() {
        return column1;
    }

    public void setColumn1(String column1) {
        this.column1 = column1;
    }

    public String getColumn2() {
        return column2;
    }

    public void setColumn2(String column2) {
        this.column2 = column2;
    }

    public String getColumn3() {
        return column3;
    }

    public void setColumn3(String column3) {
        this.column3 = column3;
    }
}
