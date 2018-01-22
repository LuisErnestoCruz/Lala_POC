/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.database;

import com.dobleo.rpa.models.Branch;
import com.dobleo.rpa.models.Document;
import com.dobleo.rpa.models.Link;
import com.dobleo.rpa.models.Reception;
import com.dobleo.rpa.models.Sale;
import com.novayre.jidoka.client.api.IJidokaServer;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author X220
 */
public class DatabaseUtilities {
    
    private Map<Integer, String> documentColumns;
    private Map<Integer, String> receptionsColumns;
    private Map<Integer, String> virtualReceptionsColumns;
    private Map<Integer, String> salesColumns;
    private Map<Integer, String> branchColumns;
    private Map<Integer, String> virtualSalesColumns;
    private Map<Integer, String> virtualTableColumns;

    public Map<Integer, String> getDocumentColumns() {
        return documentColumns;
    }

    public void setDocumentColumns(Map<Integer, String> documentColumns) {
        this.documentColumns = documentColumns;
    }

    public Map<Integer, String> getReceptionsColumns() {
        return receptionsColumns;
    }

    public void setReceptionsColumns(Map<Integer, String> receptionsColumns) {
        this.receptionsColumns = receptionsColumns;
    }

    public Map<Integer, String> getSalesColumns() {
        return salesColumns;
    }

    public void setSalesColumns(Map<Integer, String> salesColumns) {
        this.salesColumns = salesColumns;
    }

    public Map<Integer, String> getVirtualReceptionsColumns() {
        return virtualReceptionsColumns;
    }

    public void setVirtualReceptionsColumns(Map<Integer, String> virtualReceptionsColumns) {
        this.virtualReceptionsColumns = virtualReceptionsColumns;
    }

    public Map<Integer, String> getVirtualSalesColumns() {
        return virtualSalesColumns;
    }

    public void setVirtualSalesColumns(Map<Integer, String> virtualSalesColumns) {
        this.virtualSalesColumns = virtualSalesColumns;
    }

    public Map<Integer, String> getVirtualTableColumns() {
        return virtualTableColumns;
    }

    public void setVirtualTableColumns(Map<Integer, String> virtualTableColumns) {
        this.virtualTableColumns = virtualTableColumns;
    }

    public Map<Integer, String> getBranchColumns() {
        return branchColumns;
    }

    public void setBranchColumns(Map<Integer, String> branchColumns) {
        this.branchColumns = branchColumns;
    }
    
    public static String createNewDatabase(IJidokaServer<?> server, String dbname) {
        String url = null;
        try {
            Class.forName("org.sqlite.JDBC");
            //System.out.println("Existe la Clase");
            server.info("Exist Class org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            //System.out.println("Error: " + e.getMessage());
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        try {
            //File file = new File (".");
            //url = file.getCanonicalPath(); 
            //url = url.replace("\\", "/");
            url = "jdbc:sqlite:" + url + "/" + dbname + ".db";
            url = "jdbc:sqlite:" + Paths.get(server.getCurrentDir()).toRealPath() + "/" + dbname + ".db";
            //url = "jdbc:sqlite:" + url + "/" + dbname + ".db";
        } catch (Exception e) {
            //System.out.println("Error: " + e.getMessage());
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        
        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                //System.out.println("db has been created");
                server.info("Database has been created"); 
            }
        } catch (SQLException e) 
        {
            //System.out.println("Error: " + e.getMessage());
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return url;
    }
    
    public static Connection connectDatabase(IJidokaServer<?> server, String url) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
            //server.info("Connection to SQLite has been established.");
            //System.out.println("Connection to SQLite has been established.");

        } catch (SQLException e) {
            //System.out.println("Error: " + e.getMessage()); 
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return conn;
    }
    
    public static void createTable(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns) {
        try {
            Connection connection = connectDatabase(server, url);
            Statement statement = connection.createStatement();
            StringBuilder createTableQuery = new StringBuilder();
            createTableQuery.append("CREATE TABLE IF NOT EXISTS ");
            createTableQuery.append(tableName);
            createTableQuery.append(" (");
            String fields = getTableColumnInformation(tableName, columns);
            /*String fields = headers.values().stream()
                    .map(s -> {
                        switch(s) {
                            case "id": return s + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT";
                            case "numero_folio":
                            case "nombre":
                                return s + " TEXT NOT NULL";
                            default:
                                return s + " TEXT NOT NULL";
                        }
                    }).collect(Collectors.joining(","));*/
            createTableQuery.append(fields);
            createTableQuery.append(")");
            statement.executeUpdate(createTableQuery.toString());
            statement.close();
            connection.close();
            //System.out.println("Creacion de Tabla Exitosamente");
            server.info("Creation of Successful Table " + tableName);
        } catch (SQLException e) {
            //System.out.println("Error: " + e.getMessage());
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
            
        }
    }
    
    public static void createVirtualTable(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns)
    {
        try {
            Connection connection = connectDatabase(server, url);
            Statement statement = connection.createStatement();
            StringBuilder createTableQuery = new StringBuilder();
            createTableQuery.append("CREATE VIRTUAL TABLE IF NOT EXISTS ");
            createTableQuery.append(tableName);
            createTableQuery.append(" USING FTS5(");
            String fields = getTableColumnInformation(tableName, columns);
            /*String fields = headers.values().stream()
                    .map(s -> {
                        switch(s) {
                            case "id": return s + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT";
                            case "numero_folio":
                            case "nombre":
                                return s + " TEXT NOT NULL";
                            default:
                                return s + " TEXT NOT NULL";
                        }
                    }).collect(Collectors.joining(","));*/
            createTableQuery.append(fields);
            createTableQuery.append(");");
            statement.executeUpdate(createTableQuery.toString());
            statement.close();
            connection.close();
            //System.out.println("Creacion de Tabla Exitosamente");
            server.info("Creation of Successful Table " + tableName);
        } catch (SQLException e) {
            //System.out.println("Error: " + e.getMessage());
            server.error(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
            
        }
    }
    
    public static String getTableColumnInformation(String tableName, Map<Integer, String> columnName)
    {
        String tableColumnInformation = "";
        if(tableName != null)
        {
            tableName = tableName.replace(" ", "");
            if(tableName.length() > 0)
            {
                switch(tableName)
                {
                    case "documento": tableColumnInformation = documentTable(columnName); break;
                    case "recepciones": tableColumnInformation = receptionTable(columnName); break;
                    case "ventas": tableColumnInformation = saleTable(columnName); break;
                    case "sucursales": tableColumnInformation = branchTable(columnName); break; 
                    case "virtual_recepciones": tableColumnInformation = virtualReceptionTable(columnName); break;
                    case "virtual_ventas": tableColumnInformation = virtualSaleTable(columnName); break;
                    case "virtual_amarre": tableColumnInformation = virtualMoorageTable(columnName); break;
                    default: tableColumnInformation = ""; break;
                }
            }
        }
        return tableColumnInformation;
    }
    
    public static String documentTable(Map<Integer, String> columns)
    {
        String columnParameters = ""; 
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: columnParameters += columns.get(a) + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"; break;
                        case 1: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 2: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 3: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        default: columnParameters += " TEXT NOT NULL,"; break; 
                    }
                }
                columnParameters = columnParameters.substring(0, columnParameters.lastIndexOf(","));
            }
        }
        return columnParameters;
    }
    
    public static String receptionTable(Map<Integer, String> columns)
    {
        String columnParameters = ""; 
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: columnParameters += columns.get(a) + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"; break;
                        case 1: columnParameters += " " + columns.get(a) + " INTEGER NOT NULL,"; break;
                        case 2: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 3: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 4: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 5: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 6: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 7: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 8: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 9: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 10: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 11: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        case 12: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        case 13: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        default: columnParameters += " TEXT NOT NULL,"; break; 
                    }
                }
                columnParameters += " FOREIGN KEY(`idFolio`) REFERENCES `documento`(`id`)";
            }
        }
        return columnParameters;
    }
    
    public static String virtualReceptionTable(Map<Integer, String> columns)
    {
        String columnParameters = ""; 
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        /*case 0: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 1: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 2: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 3: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 4: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 5: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 6: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 7: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 8: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 9: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 10: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 11: columnParameters += " " + columns.get(a) + " TEXT"; break;*/
                        
                        case 0: columnParameters += " " + columns.get(a) + ","; break;
                        case 1: columnParameters += " " + columns.get(a) + ","; break;
                        case 2: columnParameters += " " + columns.get(a) + ","; break;
                        case 3: columnParameters += " " + columns.get(a) + ","; break;
                        case 4: columnParameters += " " + columns.get(a) + ","; break;
                        case 5: columnParameters += " " + columns.get(a) + ","; break;
                        case 6: columnParameters += " " + columns.get(a) + ","; break;
                        case 7: columnParameters += " " + columns.get(a) + ","; break;
                        case 8: columnParameters += " " + columns.get(a) + ","; break;
                        case 9: columnParameters += " " + columns.get(a) + ","; break;
                        case 10: columnParameters += " " + columns.get(a) + ","; break;
                        case 11: columnParameters += " " + columns.get(a) + ","; break;
                        case 12: columnParameters += " " + columns.get(a) + ""; break;
                        default: columnParameters += ","; break; 
                    }
                }
                //columnParameters += " FOREIGN KEY(`idFolio`) REFERENCES `documento`(`id`)";
            }
        }
        return columnParameters;
    }
    
    public static void insertIntoDocument(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns, Document document)
    {
        try
        {
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(); 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("INSERT INTO documento");
            sqlQuery.append(" (");
            if(columns != null)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    sqlQuery.append(columns.get(a));
                    sqlQuery.append(",");
                }
                sqlQuery.deleteCharAt(sqlQuery.lastIndexOf(","));
            }
            
            sqlQuery.append(")"); 
            sqlQuery.append(" VALUES");
            sqlQuery.append(" (");
            sqlQuery.append("NULL");
            sqlQuery.append(",");
            sqlQuery.append("'" + document.getNumeroFolio() + "'"); 
            sqlQuery.append(","); 
            sqlQuery.append("'"+ document.getNombre() + "'");
            sqlQuery.append(",");
            sqlQuery.append("" + document.getPorcentajeIncidencia() + ""); 
            sqlQuery.append(");");
            statement.executeUpdate(sqlQuery.toString());
            statement.close();
            connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static void insertIntoReception(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns, ArrayList<Reception> listReception)
    {
        try
        {
            int times = 0;
            String receptionInformation = "";
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            //Statement statement = connection.createStatement(); 
            PreparedStatement prepareStatement = null; 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("INSERT INTO recepciones");
            sqlQuery.append(" (");
            if(columns != null)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    sqlQuery.append(columns.get(a));
                    sqlQuery.append(",");
                }
                sqlQuery.deleteCharAt(sqlQuery.lastIndexOf(","));
            }
            
            sqlQuery.append(")"); 
            sqlQuery.append(" VALUES");
            sqlQuery.append(" (");
            receptionInformation = receptionInformation(columns);
            sqlQuery.append(receptionInformation);
            sqlQuery.append(");");
            prepareStatement = connection.prepareStatement(sqlQuery.toString());
            
            for(Reception reception: listReception)
            {
                prepareStatement.setInt(1, reception.getIdFolio());
                prepareStatement.setString(2, reception.getMtvo());
                prepareStatement.setString(3, reception.getTienda());
                prepareStatement.setString(4, reception.getTienda2());
                prepareStatement.setString(5, reception.getRecibo());
                prepareStatement.setString(6, reception.getOrden());
                prepareStatement.setString(7, reception.getAdicional());
                prepareStatement.setString(8, reception.getRemision());
                prepareStatement.setString(9, reception.getFecha());
                prepareStatement.setString(10, reception.getFecha2());
                prepareStatement.setString(11, reception.getValor());
                prepareStatement.setString(12, reception.getIva());
                prepareStatement.setString(13, reception.getNeto());
                prepareStatement.addBatch();
                times++;
                
                if(times % 1000 == 0 || times == listReception.size())
                {
                    prepareStatement.executeBatch();
                }
            }
            //prepareStatement.executeUpdate();
            //statement.executeUpdate(sqlQuery.toString());
            //statement.close();
            prepareStatement.executeBatch();
            prepareStatement.close();
            connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static void insertIntoVirtualReception(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns, ArrayList<Reception> listReception)
    {
        try
        {
            int times = 0;
            String virtualReceptionInformation = "";
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            //Statement statement = connection.createStatement(); 
            PreparedStatement prepareStatement = null; 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("INSERT INTO virtual_recepciones");
            sqlQuery.append(" (");
            if(columns != null)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    sqlQuery.append(columns.get(a));
                    sqlQuery.append(",");
                }
                sqlQuery.deleteCharAt(sqlQuery.lastIndexOf(","));
            }
            
            sqlQuery.append(")"); 
            sqlQuery.append(" VALUES");
            sqlQuery.append(" (");
            virtualReceptionInformation = virtualReceptionInformation(columns);
            sqlQuery.append(virtualReceptionInformation);
            sqlQuery.append(");");
            prepareStatement = connection.prepareStatement(sqlQuery.toString());
            
            for(Reception reception: listReception)
            {
                prepareStatement.setString(1, String.valueOf(reception.getIdFolio()));
                prepareStatement.setString(2, reception.getMtvo());
                prepareStatement.setString(3, reception.getTienda());
                prepareStatement.setString(4, reception.getTienda2());
                prepareStatement.setString(5, reception.getRecibo());
                prepareStatement.setString(6, reception.getOrden());
                prepareStatement.setString(7, reception.getAdicional());
                prepareStatement.setString(8, reception.getRemision());
                prepareStatement.setString(9, reception.getFecha());
                prepareStatement.setString(10, reception.getFecha2());
                prepareStatement.setString(11, reception.getValor());
                prepareStatement.setString(12, reception.getIva());
                prepareStatement.setString(13, reception.getNeto());
                prepareStatement.addBatch();
                times++;
                
                if(times % 1000 == 0 || times == listReception.size())
                {
                    prepareStatement.executeBatch();
                }
            }
            prepareStatement.executeBatch();
            prepareStatement.close();
            connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static void insertIntoSale(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns, ArrayList<Sale> listSale)
    {
        try
        {
            int times = 0;
            String saleInformation = "";
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement prepareStatement = null;
            //Statement statement = connection.createStatement(); 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("INSERT INTO ventas");
            sqlQuery.append(" (");
            if(columns != null)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    sqlQuery.append(columns.get(a));
                    sqlQuery.append(",");
                }
                sqlQuery.deleteCharAt(sqlQuery.lastIndexOf(","));
            }
            
            sqlQuery.append(")"); 
            sqlQuery.append(" VALUES");
            sqlQuery.append(" (");
            saleInformation = saleInformation(columns);
            sqlQuery.append(saleInformation);
            sqlQuery.append(");");
            //server.info("Query Insert: " + sqlQuery.toString()); 
            prepareStatement = connection.prepareStatement(sqlQuery.toString());
            for(Sale sale: listSale)
            {
                prepareStatement.setInt(1, sale.getIdFolio());
                prepareStatement.setString(2, sale.getFecha());
                prepareStatement.setString(3, sale.getFecha2());
                prepareStatement.setString(4, sale.getPedidoAdicional());
                prepareStatement.setString(5, sale.getFactura());
                prepareStatement.setString(6, sale.getFolio());
                prepareStatement.setString(7, sale.getSolicitante());
                prepareStatement.setString(8, sale.getCedis());
                prepareStatement.setString(9, sale.getDestinatario());
                prepareStatement.setString(10, sale.getNombreDestinatario());
                prepareStatement.setString(11, sale.getNombreDestinatario2());
                prepareStatement.setString(12, sale.getFacturaRemisionSicav());
                if(StringUtils.isBlank(sale.getImporte())) { prepareStatement.setDouble(13, 0.00);} else { prepareStatement.setDouble(13, Double.parseDouble(sale.getImporte())); }
                prepareStatement.setString(14, sale.getCliente());
                prepareStatement.setString(15, sale.getRefFact());
                prepareStatement.setString(16, sale.getReferencia());
                prepareStatement.setString(17, sale.getClvRef2());
                prepareStatement.setString(18, sale.getClvRef3());
                prepareStatement.setString(19, sale.getFechaDoc());
                prepareStatement.setString(20, sale.getFechaDoc2());
                prepareStatement.setString(21, sale.getVencNeto());
                prepareStatement.setString(22, sale.getVencNeto2());
                if(StringUtils.isBlank(sale.getImpteMl())) { prepareStatement.setString(23, null);} else { prepareStatement.setDouble(23, Double.parseDouble(sale.getImpteMl())); }
                prepareStatement.setString(24, sale.getCe());
                prepareStatement.setString(25, sale.getDiv());
                prepareStatement.addBatch();
                times++;
                
                if(times % 1000 == 0 || times == listSale.size())
                {
                    prepareStatement.executeBatch();
                }
            }
            
            //prepareStatement.executeUpdate();
            //statement.executeUpdate(sqlQuery.toString());
            //statement.close();
            prepareStatement.executeBatch();
            prepareStatement.close();
            connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static void insertIntoVirtualSale(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns, ArrayList<Sale> listSale)
    {
        try
        {
            int times = 0;
            String virtualSaleInformation = "";
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement prepareStatement = null;
            //Statement statement = connection.createStatement(); 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("INSERT INTO virtual_ventas");
            sqlQuery.append(" (");
            if(columns != null)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    sqlQuery.append(columns.get(a));
                    sqlQuery.append(",");
                }
                sqlQuery.deleteCharAt(sqlQuery.lastIndexOf(","));
            }
            
            sqlQuery.append(")"); 
            sqlQuery.append(" VALUES");
            sqlQuery.append(" (");
            virtualSaleInformation = virtualSaleInformation(columns);
            sqlQuery.append(virtualSaleInformation);
            sqlQuery.append(");");
            //server.info("Query Insert: " + sqlQuery.toString()); 
            prepareStatement = connection.prepareStatement(sqlQuery.toString());
            for(Sale sale: listSale)
            {
                prepareStatement.setString(1, String.valueOf(sale.getIdFolio()));
                prepareStatement.setString(2, sale.getFecha());
                prepareStatement.setString(3, sale.getFecha2());
                prepareStatement.setString(4, sale.getPedidoAdicional());
                prepareStatement.setString(5, sale.getFactura());
                prepareStatement.setString(6, sale.getFolio());
                prepareStatement.setString(7, sale.getSolicitante());
                prepareStatement.setString(8, sale.getCedis());
                prepareStatement.setString(9, sale.getDestinatario());
                prepareStatement.setString(10, sale.getNombreDestinatario());
                prepareStatement.setString(11, sale.getNombreDestinatario2());
                prepareStatement.setString(12, sale.getFacturaRemisionSicav());
                prepareStatement.setString(13, sale.getImporte());
                prepareStatement.setString(14, sale.getCliente());
                prepareStatement.setString(15, sale.getRefFact());
                prepareStatement.setString(16, sale.getReferencia());
                prepareStatement.setString(17, sale.getClvRef2());
                prepareStatement.setString(18, sale.getClvRef3());
                prepareStatement.setString(19, sale.getFechaDoc());
                prepareStatement.setString(20, sale.getFechaDoc2());
                prepareStatement.setString(21, sale.getVencNeto());
                prepareStatement.setString(22, sale.getVencNeto2());
                if(StringUtils.isBlank(sale.getImpteMl())) { prepareStatement.setString(23, null);} else { prepareStatement.setString(23, sale.getImpteMl()); }
                prepareStatement.setString(24, sale.getCe());
                prepareStatement.setString(25, sale.getDiv());
                prepareStatement.addBatch();
                times++;
                
                if(times % 1000 == 0 || times == listSale.size())
                {
                    prepareStatement.executeBatch();
                }
            }
            
            prepareStatement.executeBatch();
            prepareStatement.close();
            connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static void insertIntoBranch(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns, ArrayList<Branch> listBranch)
    {
        try
        {
            int times = 0;
            String branchInformation = "";
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement prepareStatement = null;
            //Statement statement = connection.createStatement(); 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("INSERT INTO sucursales");
            sqlQuery.append(" (");
            if(columns != null)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    sqlQuery.append(columns.get(a));
                    sqlQuery.append(",");
                }
                sqlQuery.deleteCharAt(sqlQuery.lastIndexOf(","));
            }
            
            sqlQuery.append(")"); 
            sqlQuery.append(" VALUES");
            sqlQuery.append(" (");
            branchInformation = branchInformation(columns);
            sqlQuery.append(branchInformation);
            sqlQuery.append(");");
            //server.info("Query Insert: " + sqlQuery.toString()); 
            prepareStatement = connection.prepareStatement(sqlQuery.toString());
            for(Branch branch: listBranch)
            {
                prepareStatement.setString(1, branch.getAnalista());
                prepareStatement.setString(2, branch.getCliente());
                prepareStatement.setString(3, branch.getZona());
                prepareStatement.setString(4, branch.getCentro());
                prepareStatement.setString(5, branch.getCedis());
                prepareStatement.setString(6, branch.getSucursalSAP());
                prepareStatement.setString(7, branch.getSucursalLALA());
                prepareStatement.setString(8, branch.getSucursalLALA2());
                prepareStatement.setString(9, branch.getCrLALA());
                prepareStatement.setString(10, branch.getPlaza1());
                prepareStatement.setString(11, branch.getCrOXXO());
                prepareStatement.setString(12, branch.getPlaza2());
                prepareStatement.setString(13, branch.getSucursalOXXO());
                prepareStatement.setString(14, branch.getSucursalOXXO2());
                prepareStatement.setString(15, branch.getLiquidacion());
                prepareStatement.setString(16, branch.getVentaCruzada());
                prepareStatement.addBatch();
                times++;
                
                if(times % 1000 == 0 || times == listBranch.size())
                {
                    prepareStatement.executeBatch();
                }
            }
            
            //prepareStatement.executeUpdate();
            //statement.executeUpdate(sqlQuery.toString());
            //statement.close();
            prepareStatement.executeBatch();
            prepareStatement.close();
            connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static void insertIntoVirtualMoorage(IJidokaServer<?> server, String url, String tableName, Map<Integer, String> columns, ArrayList<Link> listLink)
    {
        try
        {
            int times = 0;
            String virtualMoorageInformation = "";
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement prepareStatement = null;
            //Statement statement = connection.createStatement(); 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("INSERT INTO virtual_amarre");
            sqlQuery.append(" (");
            if(columns != null)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    sqlQuery.append(columns.get(a));
                    sqlQuery.append(",");
                }
                sqlQuery.deleteCharAt(sqlQuery.lastIndexOf(","));
            }
            
            sqlQuery.append(")"); 
            sqlQuery.append(" VALUES");
            sqlQuery.append(" (");
            virtualMoorageInformation = virtualMoorageInformation(columns);
            sqlQuery.append(virtualMoorageInformation);
            sqlQuery.append(");");
            //server.info("Query Insert: " + sqlQuery.toString()); 
            prepareStatement = connection.prepareStatement(sqlQuery.toString());
            for(Link link: listLink)
            {
                prepareStatement.setString(1, String.valueOf(link.getRecepcion().getId()));
                prepareStatement.setString(2, String.valueOf(link.getRecepcion().getIdFolio()));
                prepareStatement.setString(3, link.getRecepcion().getMtvo());
                prepareStatement.setString(4, link.getRecepcion().getTienda());
                prepareStatement.setString(5, link.getRecepcion().getTienda2());
                prepareStatement.setString(6, link.getRecepcion().getRecibo());
                prepareStatement.setString(7, link.getRecepcion().getOrden());
                prepareStatement.setString(8, link.getRecepcion().getAdicional());
                prepareStatement.setString(9, link.getRecepcion().getRemision());
                prepareStatement.setString(10, link.getRecepcion().getFecha());
                prepareStatement.setString(11, link.getRecepcion().getFecha2());
                prepareStatement.setString(12, link.getRecepcion().getValor());
                prepareStatement.setString(13, link.getRecepcion().getIva());
                prepareStatement.setString(14, link.getRecepcion().getNeto());
                prepareStatement.setString(15, String.valueOf(link.getVenta().getId()));
                prepareStatement.setString(16, String.valueOf(link.getVenta().getIdFolio()));
                prepareStatement.setString(17, link.getVenta().getFecha());
                prepareStatement.setString(18, link.getVenta().getFecha2());
                prepareStatement.setString(19, link.getVenta().getPedidoAdicional());
                prepareStatement.setString(20, link.getVenta().getFactura());
                prepareStatement.setString(21, link.getVenta().getFolio());
                prepareStatement.setString(22, link.getVenta().getSolicitante());
                prepareStatement.setString(23, link.getVenta().getCedis());
                prepareStatement.setString(24, link.getVenta().getDestinatario());
                prepareStatement.setString(25, link.getVenta().getNombreDestinatario());
                prepareStatement.setString(26, link.getVenta().getNombreDestinatario2());
                prepareStatement.setString(27, link.getVenta().getFacturaRemisionSicav());
                prepareStatement.setString(28, link.getVenta().getImporte());
                prepareStatement.setString(29, link.getVenta().getCliente());
                prepareStatement.setString(30, link.getVenta().getRefFact());
                prepareStatement.setString(31, link.getVenta().getReferencia());
                prepareStatement.setString(32, link.getVenta().getClvRef2());
                prepareStatement.setString(33, link.getVenta().getClvRef3());
                prepareStatement.setString(34, link.getVenta().getFechaDoc());
                prepareStatement.setString(35, link.getVenta().getFechaDoc2());
                prepareStatement.setString(36, link.getVenta().getVencNeto());
                prepareStatement.setString(37, link.getVenta().getVencNeto2());
                prepareStatement.setString(38, link.getVenta().getImpteMl());
                prepareStatement.setString(39, link.getVenta().getCe());
                prepareStatement.setString(40, link.getVenta().getDiv());
                prepareStatement.setString(41, String.valueOf(link.getDocumento().getId()));
                prepareStatement.setString(42, link.getDocumento().getNombre());
                prepareStatement.setString(43, link.getDocumento().getNumeroFolio());
                prepareStatement.setString(44, link.getDocumento().getPorcentajeIncidencia());
                prepareStatement.addBatch();
                times++;
                
                if(times % 1000 == 0 || times == listLink.size())
                {
                    prepareStatement.executeBatch();
                }
            }
            
            prepareStatement.executeBatch();
            prepareStatement.close();
            connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static String receptionInformation(Map<Integer, String> columns)
    {
        String receptionInformation = "";
        //if(columns != null && reception != null)
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        /*case 0: receptionInformation += "NULL,"; break;
                        case 1: receptionInformation += "" + reception.getIdFolio() + ","; break;
                        case 2: receptionInformation += "'" + reception.getMtvo() + "',"; break;
                        case 3: receptionInformation += "'" + reception.getTienda() + "',"; break;
                        case 4: receptionInformation += "'" + reception.getRecibo() + "',"; break;
                        case 5: receptionInformation +=  "'" + reception.getOrden() + "',"; break;
                        case 6: receptionInformation += "'" + reception.getAdicional() + "',"; break;
                        case 7: receptionInformation += "'" + reception.getRemision() + "',"; break;
                        case 8: receptionInformation += "'" + reception.getFecha() + "',"; break;
                        case 9: receptionInformation += reception.getValor() + ","; break;
                        case 10: receptionInformation += reception.getIva() + ","; break;
                        case 11: receptionInformation += reception.getNeto(); break;
                        default: receptionInformation += ""; break;*/
                        case 0: receptionInformation += "NULL,"; break;
                        case 1: receptionInformation += "?,"; break;
                        case 2: receptionInformation += "?,"; break;
                        case 3: receptionInformation += "?,"; break;
                        case 4: receptionInformation += "?,"; break;
                        case 5: receptionInformation += "?,"; break;
                        case 6: receptionInformation += "?,"; break;
                        case 7: receptionInformation += "?,"; break;
                        case 8: receptionInformation += "?,"; break;
                        case 9: receptionInformation += "?,"; break;
                        case 10: receptionInformation += "?,"; break;
                        case 11: receptionInformation += "?,"; break;
                        case 12: receptionInformation += "?,"; break;
                        case 13: receptionInformation += "?"; break;
                        default: receptionInformation += "?"; break;
                        
                    }
                }
            }
        }
        return receptionInformation;
    }
    
    public static String virtualReceptionInformation(Map<Integer, String> columns)
    {
        String virtualReceptionInformation = "";
        //if(columns != null && reception != null)
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: virtualReceptionInformation += "?,"; break;
                        case 1: virtualReceptionInformation += "?,"; break;
                        case 2: virtualReceptionInformation += "?,"; break;
                        case 3: virtualReceptionInformation += "?,"; break;
                        case 4: virtualReceptionInformation += "?,"; break;
                        case 5: virtualReceptionInformation += "?,"; break;
                        case 6: virtualReceptionInformation += "?,"; break;
                        case 7: virtualReceptionInformation += "?,"; break;
                        case 8: virtualReceptionInformation += "?,"; break;
                        case 9: virtualReceptionInformation += "?,"; break;
                        case 10: virtualReceptionInformation += "?,"; break;
                        case 11: virtualReceptionInformation += "?,"; break;
                        case 12: virtualReceptionInformation += "?"; break;
                        default: virtualReceptionInformation += "?"; break;
                        
                    }
                }
            }
        }
        return virtualReceptionInformation;
    }
    
    public static String saleInformation(Map<Integer, String> columns)
    {
        String saleInformation = "";
        //if(columns != null && sale != null)
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: saleInformation += "NULL,"; break;
                        case 1: saleInformation += "?,"; break;
                        case 2: saleInformation += "?,"; break;
                        case 3: saleInformation += "?,"; break;
                        case 4: saleInformation += "?,"; break;
                        case 5: saleInformation += "?,"; break;
                        case 6: saleInformation += "?,"; break; 
                        case 7: saleInformation += "?,"; break; 
                        case 8: saleInformation += "?,"; break;
                        case 9: saleInformation += "?,"; break;
                        case 10: saleInformation += "?,"; break;
                        case 11: saleInformation += "?,"; break;
                        case 12: saleInformation += "?,"; break;
                        case 13: saleInformation += "?,"; break;
                        case 14: saleInformation += "?,"; break; 
                        case 15: saleInformation += "?,"; break;
                        case 16: saleInformation += "?,"; break;
                        case 17: saleInformation += "?,"; break;
                        case 18: saleInformation += "?,"; break;
                        case 19: saleInformation += "?,"; break;
                        case 20: saleInformation += "?,"; break;
                        case 21: saleInformation += "?,"; break;
                        case 22: saleInformation += "?,"; break;
                        case 23: saleInformation += "?,"; break;
                        case 24: saleInformation += "?,"; break;
                        case 25: saleInformation += "?"; break;
                        default: saleInformation += "?"; break;
                        /*case 1: saleInformation += "" + sale.getIdFolio() + ","; break;
                        case 2: saleInformation += "'" + sale.getFecha() + "',"; break;
                        case 3: saleInformation += "'" + sale.getPedidoAdicional() + "',"; break;
                        case 4: saleInformation += "'" + sale.getFactura() + "',"; break;
                        case 5: saleInformation += "'" + sale.getFolio() + "',"; break;
                        case 6: saleInformation += "'" + sale.getSolicitante() + "',"; break; 
                        case 7: saleInformation += "'" + sale.getCedis() + "',"; break; 
                        case 8: saleInformation += "'" + sale.getDestinatario() + "',"; break;
                        case 9: saleInformation += "'" + sale.getNombreDestinatario() + "',"; break;
                        case 10: saleInformation += "'" + sale.getFacturaRemisionSicav() + "',"; break;
                        case 11: saleInformation += sale.getImporte() + ","; break;
                        case 12: saleInformation += "'" + sale.getCliente() + "',"; break;
                        case 13: saleInformation += "'" + sale.getRefFact() + "',"; break; 
                        case 14: saleInformation += "'" + sale.getReferencia() + "',"; break;
                        case 15: saleInformation += "'" + sale.getClvRef2() + "',"; break;
                        case 16: saleInformation += "'" + sale.getClvRef3() + "',"; break;
                        case 17: saleInformation += "'" + sale.getFechaDoc() + "',"; break;
                        case 18: saleInformation += "'" + sale.getVencNeto() + "',"; break;
                        case 19: saleInformation += sale.getImpteMl() + ","; break;
                        case 20: saleInformation += "'" + sale.getCe() + "',"; break;
                        case 21: saleInformation += "'" + sale.getDiv() + "'"; break;
                        default: saleInformation += ""; break; */
                    }
                }
            }
        }
        return saleInformation; 
    }
    
    public static String virtualSaleInformation(Map<Integer, String> columns)
    {
        String virtualSaleInformation = "";
        //if(columns != null && sale != null)
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: virtualSaleInformation += "?,"; break;
                        case 1: virtualSaleInformation += "?,"; break;
                        case 2: virtualSaleInformation += "?,"; break;
                        case 3: virtualSaleInformation += "?,"; break;
                        case 4: virtualSaleInformation += "?,"; break;
                        case 5: virtualSaleInformation += "?,"; break; 
                        case 6: virtualSaleInformation += "?,"; break; 
                        case 7: virtualSaleInformation += "?,"; break;
                        case 8: virtualSaleInformation += "?,"; break;
                        case 9: virtualSaleInformation += "?,"; break;
                        case 10: virtualSaleInformation += "?,"; break;
                        case 11: virtualSaleInformation += "?,"; break;
                        case 12: virtualSaleInformation += "?,"; break; 
                        case 13: virtualSaleInformation += "?,"; break;
                        case 14: virtualSaleInformation += "?,"; break;
                        case 15: virtualSaleInformation += "?,"; break;
                        case 16: virtualSaleInformation += "?,"; break;
                        case 17: virtualSaleInformation += "?,"; break;
                        case 18: virtualSaleInformation += "?,"; break;
                        case 19: virtualSaleInformation += "?,"; break;
                        case 20: virtualSaleInformation += "?,"; break;
                        case 21: virtualSaleInformation += "?,"; break;
                        case 22: virtualSaleInformation += "?,"; break;
                        case 23: virtualSaleInformation += "?,"; break;
                        case 24: virtualSaleInformation += "?"; break;
                        default: virtualSaleInformation += "?"; break;
                    }
                }
            }
        }
        return virtualSaleInformation; 
    }
    
    public static String virtualMoorageInformation(Map<Integer, String> columns)
    {
        String virtualMoorageInformation = "";
        //if(columns != null && sale != null)
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: virtualMoorageInformation += "?,"; break;
                        case 1: virtualMoorageInformation += "?,"; break;
                        case 2: virtualMoorageInformation += "?,"; break;
                        case 3: virtualMoorageInformation += "?,"; break;
                        case 4: virtualMoorageInformation += "?,"; break;
                        case 5: virtualMoorageInformation += "?,"; break; 
                        case 6: virtualMoorageInformation += "?,"; break; 
                        case 7: virtualMoorageInformation += "?,"; break;
                        case 8: virtualMoorageInformation += "?,"; break;
                        case 9: virtualMoorageInformation += "?,"; break;
                        case 10: virtualMoorageInformation += "?,"; break;
                        case 11: virtualMoorageInformation += "?,"; break;
                        case 12: virtualMoorageInformation += "?,"; break; 
                        case 13: virtualMoorageInformation += "?,"; break;
                        case 14: virtualMoorageInformation += "?,"; break;
                        case 15: virtualMoorageInformation += "?,"; break;
                        case 16: virtualMoorageInformation += "?,"; break;
                        case 17: virtualMoorageInformation += "?,"; break;
                        case 18: virtualMoorageInformation += "?,"; break;
                        case 19: virtualMoorageInformation += "?,"; break;
                        case 20: virtualMoorageInformation += "?,"; break;
                        case 21: virtualMoorageInformation += "?,"; break;
                        case 22: virtualMoorageInformation += "?,"; break;
                        case 23: virtualMoorageInformation += "?,"; break;
                        case 24: virtualMoorageInformation += "?,"; break;
                        case 25: virtualMoorageInformation += "?,"; break;
                        case 26: virtualMoorageInformation += "?,"; break;
                        case 27: virtualMoorageInformation += "?,"; break;
                        case 28: virtualMoorageInformation += "?,"; break;
                        case 29: virtualMoorageInformation += "?,"; break;
                        case 30: virtualMoorageInformation += "?,"; break;
                        case 31: virtualMoorageInformation += "?,"; break;
                        case 32: virtualMoorageInformation += "?,"; break;
                        case 33: virtualMoorageInformation += "?,"; break;
                        case 34: virtualMoorageInformation += "?,"; break;
                        case 35: virtualMoorageInformation += "?,"; break;
                        case 36: virtualMoorageInformation += "?,"; break;
                        case 37: virtualMoorageInformation += "?,"; break;
                        case 38: virtualMoorageInformation += "?,"; break;
                        case 39: virtualMoorageInformation += "?,"; break;
                        case 40: virtualMoorageInformation += "?,"; break;
                        case 41: virtualMoorageInformation += "?,"; break;
                        case 42: virtualMoorageInformation += "?,"; break;
                        case 43: virtualMoorageInformation += "?"; break;
                        default: virtualMoorageInformation += "?"; break;
                    }
                }
            }
        }
        return virtualMoorageInformation; 
    }
    
    public static String branchInformation(Map<Integer, String> columns)
    {
        String saleInformation = "";
        //if(columns != null && sale != null)
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: saleInformation += "NULL,"; break;
                        case 1: saleInformation += "?,"; break;
                        case 2: saleInformation += "?,"; break;
                        case 3: saleInformation += "?,"; break;
                        case 4: saleInformation += "?,"; break;
                        case 5: saleInformation += "?,"; break;
                        case 6: saleInformation += "?,"; break; 
                        case 7: saleInformation += "?,"; break; 
                        case 8: saleInformation += "?,"; break;
                        case 9: saleInformation += "?,"; break;
                        case 10: saleInformation += "?,"; break;
                        case 11: saleInformation += "?,"; break;
                        case 12: saleInformation += "?,"; break;
                        case 13: saleInformation += "?,"; break;
                        case 14: saleInformation += "?,"; break;
                        case 15: saleInformation += "?,"; break;
                        case 16: saleInformation += "?"; break;
                        default: saleInformation += "?"; break;
                    }
                }
            }
        }
        return saleInformation; 
    }
    
    public static String saleTable(Map<Integer, String> columns)
    {
        String columnParameters = ""; 
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: columnParameters += columns.get(a) + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"; break;
                        case 1: columnParameters += " " + columns.get(a) + " INTEGER NOT NULL,"; break;
                        case 2: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 3: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 4: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 5: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 6: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 7: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 8: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 9: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 10: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 11: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 12: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 13: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        case 14: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 15: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 16: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 17: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 18: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 19: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 20: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 21: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 22: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 23: columnParameters += " " + columns.get(a) + " REAL,"; break;
                        case 24: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 25: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        default: columnParameters += " TEXT,"; break; 
                    }
                }
                columnParameters += " FOREIGN KEY(`idFolio`) REFERENCES `documento`(`id`)";
            }
        }
        return columnParameters;
    }
    
    public static String virtualSaleTable(Map<Integer, String> columns)
    {
        String columnParameters = ""; 
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        /*case 0: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 1: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 2: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 3: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 4: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 5: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 6: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 7: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 8: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 9: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 10: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 11: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 12: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 13: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 14: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 15: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 16: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 17: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 18: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 19: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 20: columnParameters += " " + columns.get(a) + " TEXT"; break;*/
                        
                        case 0: columnParameters += " " + columns.get(a) + ","; break;
                        case 1: columnParameters += " " + columns.get(a) + ","; break;
                        case 2: columnParameters += " " + columns.get(a) + ","; break;
                        case 3: columnParameters += " " + columns.get(a) + ","; break;
                        case 4: columnParameters += " " + columns.get(a) + ","; break;
                        case 5: columnParameters += " " + columns.get(a) + ","; break;
                        case 6: columnParameters += " " + columns.get(a) + ","; break;
                        case 7: columnParameters += " " + columns.get(a) + ","; break;
                        case 8: columnParameters += " " + columns.get(a) + ","; break;
                        case 9: columnParameters += " " + columns.get(a) + ","; break;
                        case 10: columnParameters += " " + columns.get(a) + ","; break;
                        case 11: columnParameters += " " + columns.get(a) + ","; break;
                        case 12: columnParameters += " " + columns.get(a) + ","; break;
                        case 13: columnParameters += " " + columns.get(a) + ","; break;
                        case 14: columnParameters += " " + columns.get(a) + ","; break;
                        case 15: columnParameters += " " + columns.get(a) + ","; break;
                        case 16: columnParameters += " " + columns.get(a) + ","; break;
                        case 17: columnParameters += " " + columns.get(a) + ","; break;
                        case 18: columnParameters += " " + columns.get(a) + ","; break;
                        case 19: columnParameters += " " + columns.get(a) + ","; break;
                        case 20: columnParameters += " " + columns.get(a) + ","; break;
                        case 21: columnParameters += " " + columns.get(a) + ","; break;
                        case 22: columnParameters += " " + columns.get(a) + ","; break;
                        case 23: columnParameters += " " + columns.get(a) + ","; break;
                        case 24: columnParameters += " " + columns.get(a) + ""; break;
                        default: columnParameters += ","; break; 
                    }
                }
                //columnParameters += " FOREIGN KEY(`idFolio`) REFERENCES `documento`(`id`)";
            }
        }
        return columnParameters;
    }
    
    public static String branchTable(Map<Integer, String> columns)
    {
        String columnParameters = ""; 
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: columnParameters += columns.get(a) + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"; break;
                        case 1: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 2: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 3: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 4: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 5: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 6: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 7: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 8: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 9: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 10: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 11: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 12: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 13: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 14: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 15: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 16: columnParameters += " " + columns.get(a) + " TEXT"; break;
                        default: columnParameters += " TEXT,"; break; 
                    }
                }
            }
        }
        return columnParameters;
    }
    
    public static String virtualMoorageTable(Map<Integer, String> columns)
    {
        String columnParameters = ""; 
        if(columns != null)
        {
            if(columns.size() > 0)
            {
                for(int a = 0; a < columns.size(); a++)
                {
                    switch(a)
                    {
                        case 0: columnParameters += " " + columns.get(a) + ","; break;
                        case 1: columnParameters += " " + columns.get(a) + ","; break;
                        case 2: columnParameters += " " + columns.get(a) + ","; break;
                        case 3: columnParameters += " " + columns.get(a) + ","; break;
                        case 4: columnParameters += " " + columns.get(a) + ","; break;
                        case 5: columnParameters += " " + columns.get(a) + ","; break;
                        case 6: columnParameters += " " + columns.get(a) + ","; break;
                        case 7: columnParameters += " " + columns.get(a) + ","; break;
                        case 8: columnParameters += " " + columns.get(a) + ","; break;
                        case 9: columnParameters += " " + columns.get(a) + ","; break;
                        case 10: columnParameters += " " + columns.get(a) + ","; break;
                        case 11: columnParameters += " " + columns.get(a) + ","; break;
                        case 12: columnParameters += " " + columns.get(a) + ","; break;
                        case 13: columnParameters += " " + columns.get(a) + ","; break;
                        case 14: columnParameters += " " + columns.get(a) + ","; break;
                        case 15: columnParameters += " " + columns.get(a) + ","; break;
                        case 16: columnParameters += " " + columns.get(a) + ","; break;
                        case 17: columnParameters += " " + columns.get(a) + ","; break;
                        case 18: columnParameters += " " + columns.get(a) + ","; break;
                        case 19: columnParameters += " " + columns.get(a) + ","; break;
                        case 20: columnParameters += " " + columns.get(a) + ","; break;
                        case 21: columnParameters += " " + columns.get(a) + ","; break;
                        case 22: columnParameters += " " + columns.get(a) + ","; break;
                        case 23: columnParameters += " " + columns.get(a) + ","; break;
                        case 24: columnParameters += " " + columns.get(a) + ","; break;
                        case 25: columnParameters += " " + columns.get(a) + ","; break;
                        case 26: columnParameters += " " + columns.get(a) + ","; break;
                        case 27: columnParameters += " " + columns.get(a) + ","; break;
                        case 28: columnParameters += " " + columns.get(a) + ","; break;
                        case 29: columnParameters += " " + columns.get(a) + ","; break;
                        case 30: columnParameters += " " + columns.get(a) + ","; break;
                        case 31: columnParameters += " " + columns.get(a) + ","; break;
                        case 32: columnParameters += " " + columns.get(a) + ","; break;
                        case 33: columnParameters += " " + columns.get(a) + ","; break;
                        case 34: columnParameters += " " + columns.get(a) + ","; break;
                        case 35: columnParameters += " " + columns.get(a) + ","; break;
                        case 36: columnParameters += " " + columns.get(a) + ","; break;
                        case 37: columnParameters += " " + columns.get(a) + ","; break;
                        case 38: columnParameters += " " + columns.get(a) + ","; break;
                        case 39: columnParameters += " " + columns.get(a) + ","; break;
                        case 40: columnParameters += " " + columns.get(a) + ","; break;
                        case 41: columnParameters += " " + columns.get(a) + ","; break;
                        case 42: columnParameters += " " + columns.get(a) + ","; break;
                        case 43: columnParameters += " " + columns.get(a) + ""; break;
                        default: columnParameters += ","; break; 
                    }
                }
            }
        }
        return columnParameters;
    }
    
    public int getIdFolio(IJidokaServer<?> server, String url, String tableName, Document document)
    {
        int id = 0; 
        try
        {
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(); 
            StringBuilder sqlQuery = new StringBuilder();
            sqlQuery.append("SELECT id FROM ");
            sqlQuery.append(tableName);
            sqlQuery.append(" WHERE numero_folio = '");
            sqlQuery.append(document.getNumeroFolio());
            sqlQuery.append("'");
            sqlQuery.append(" LIMIT 1");
            sqlQuery.append(";");
            server.info("Query:" + sqlQuery.toString());
            ResultSet resultSet = statement.executeQuery(sqlQuery.toString());
            while(resultSet.next())
            {
                id = resultSet.getInt("id");
            }
            resultSet.close();
            statement.close();
            //connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return id;
    }
    
    public ArrayList<Link> joinByRemissionAndAdditionalOrder(IJidokaServer<?> server, String url, int idFolio)
    {
        ArrayList<Link> listResult = new ArrayList<Link>(); 
        try
        {
            Reception reception = new Reception();
            Sale sale = new Sale();
            Link link = new Link();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            String sqlQuery = null;
            sqlQuery = "SELECT * FROM(SELECT * FROM(SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_abreviacion_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            " " +
            "UNION  " +
            " " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND ((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            " " +
            "UNION  " +
            " " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion,  " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) AND (recepciones_adicional = ventas_pedido_adicional) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            ") " +
            "GROUP BY idRecepcion " +
            ") " +
            "GROUP BY idVenta " +
            "ORDER BY ventas_destinatario";
            //server.info("Query:" + sqlQuery);
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, idFolio);
            preparedStatement.setInt(2, idFolio);
            preparedStatement.setInt(3, idFolio);
            preparedStatement.setInt(4, idFolio);
            preparedStatement.setInt(5, idFolio);
            preparedStatement.setInt(6, idFolio);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next())
            {
                link = new Link(); 
                reception = new Reception();
                sale = new Sale();
                sale.setId(resultSet.getInt("idVenta"));
                sale.setFecha(resultSet.getString("ventas_fecha"));
                sale.setPedidoAdicional(resultSet.getString("ventas_pedido_adicional"));
                sale.setFactura(resultSet.getString("ventas_factura"));
                sale.setFolio(resultSet.getString("ventas_folio"));
                sale.setSolicitante(resultSet.getString("ventas_solicitante"));
                sale.setCedis(resultSet.getString("ventas_cedis"));
                sale.setDestinatario(resultSet.getString("ventas_destinatario"));
                sale.setNombreDestinatario(resultSet.getString("ventas_nombre_destinatario"));
                sale.setFacturaRemisionSicav(resultSet.getString("ventas_remisionSicav"));
                sale.setImporte(resultSet.getString("ventas_importe"));
                reception.setId(resultSet.getInt("idRecepcion"));
                reception.setAdicional(resultSet.getString("recepciones_adicional"));
                reception.setTienda(resultSet.getString("recepciones_tienda"));
                reception.setRemision(resultSet.getString("recepciones_remision"));
                reception.setFecha(resultSet.getString("recepciones_fecha"));
                reception.setNeto(resultSet.getString("recepciones_neto"));
                link.setAbreviacionVenta(resultSet.getString("ventas_abreviacion_factura"));
                link.setDiferencia(resultSet.getString("amarre_diferencia"));
                link.setPorcentaje(resultSet.getString("amarre_porcentaje"));
                link.setBusqueda("Remision - Pedido Adicional");
                link.setRecepcion(reception);
                link.setVenta(sale);
                listResult.add(link);
            }
            resultSet.close();
            preparedStatement.close();
            //connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            listResult = new ArrayList<Link>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listResult;
    }
    
    public ArrayList<Link> searchByStoreDateAmount(IJidokaServer<?> server, String url, int idFolio, ArrayList<Link> listIdReception)
    {
        ArrayList<Link> listResult = new ArrayList<Link>(); 
        try
        {
            Reception reception = new Reception();
            Sale sale = new Sale();
            Link link = new Link();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            StringBuilder sqlSubQuery1 = new StringBuilder();
            String sqlQuery = null;
            /*sqlQuery = "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_abreviacion_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto, " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (ventas.idFolio = ?) AND (recepciones.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            //"GROUP BY idVenta " +
            "ORDER BY ventas_destinatario";*/
            sqlQuery = "SELECT * FROM(SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_abreviacion_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas.idFolio = ?) AND (recepciones.idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3)) AND NOT(recepciones_adicional = ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (ventas_nombre_destinatario2 = sucursales.sucursal_LALA2) AND (recepciones_tienda2 = sucursales.sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3) AND idRecepcion NOT IN(";
            
            for(int a = 0; a < listIdReception.size(); a++)
            {
                sqlSubQuery1.append(String.valueOf(listIdReception.get(a).getRecepcion().getId()));
                sqlSubQuery1.append(",");
            }
            
            sqlSubQuery1.deleteCharAt(sqlSubQuery1.lastIndexOf(",")); 
            sqlQuery += sqlSubQuery1.toString();
            sqlQuery += ") "; 
            sqlQuery += "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "ORDER BY ventas_destinatario";
            //server.info("Query:" + sqlQuery);
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, idFolio);
            preparedStatement.setInt(2, idFolio);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next())
            {
                link = new Link(); 
                reception = new Reception();
                sale = new Sale();
                sale.setId(resultSet.getInt("idVenta"));
                sale.setFecha(resultSet.getString("ventas_fecha"));
                sale.setPedidoAdicional(resultSet.getString("ventas_pedido_adicional"));
                sale.setFactura(resultSet.getString("ventas_factura"));
                sale.setFolio(resultSet.getString("ventas_folio"));
                sale.setSolicitante(resultSet.getString("ventas_solicitante"));
                sale.setCedis(resultSet.getString("ventas_cedis"));
                sale.setDestinatario(resultSet.getString("ventas_destinatario"));
                sale.setNombreDestinatario(resultSet.getString("ventas_nombre_destinatario"));
                sale.setFacturaRemisionSicav(resultSet.getString("ventas_remisionSicav"));
                sale.setImporte(resultSet.getString("ventas_importe"));
                reception.setId(resultSet.getInt("idRecepcion"));
                reception.setAdicional(resultSet.getString("recepciones_adicional"));
                reception.setTienda(resultSet.getString("recepciones_tienda"));
                reception.setRemision(resultSet.getString("recepciones_remision"));
                reception.setFecha(resultSet.getString("recepciones_fecha"));
                reception.setNeto(resultSet.getString("recepciones_neto"));
                link.setAbreviacionVenta(resultSet.getString("ventas_abreviacion_factura"));
                link.setDiferencia(resultSet.getString("amarre_diferencia"));
                link.setPorcentaje(resultSet.getString("amarre_porcentaje"));
                link.setBusqueda("Fecha - Tienda - Importe");
                link.setRecepcion(reception);
                link.setVenta(sale);
                listResult.add(link);
            }
            resultSet.close();
            preparedStatement.close();
            //connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            listResult = new ArrayList<Link>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listResult;
    }
    
    public ArrayList<Sale> getSalesNotMatch(IJidokaServer<?> server, String url, int idFolio, ArrayList<Link> listFirstJoin)
    {
        ArrayList<Sale> listSaleResult = new ArrayList<Sale>(); 
        try
        {
            Sale sale = new Sale();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            StringBuilder sqlSubQuery1 = new StringBuilder();
            String sqlQuery = null;
            /*sqlQuery = "SELECT * FROM ventas WHERE idFolio = ? AND id NOT IN(SELECT idVenta FROM (SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto, " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            //"GROUP BY idVenta " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            //"GROUP BY idVenta " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_adicional = ventas_pedido_adicional) " +
            //"GROUP BY idVenta " +
            "ORDER BY ventas_destinatario) " +
            "ORDER BY idVenta);";*/
            
            sqlQuery = "SELECT * FROM ventas WHERE idFolio = ? AND id NOT IN(SELECT idVenta FROM(SELECT * FROM(SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " + 
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND ((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion,  " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) AND (recepciones_adicional = ventas_pedido_adicional) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION " +
            "SELECT * FROM(SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas.idFolio = ?) AND (recepciones.idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3)) AND NOT(recepciones_adicional = ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (ventas_nombre_destinatario2 = sucursales.sucursal_LALA2) AND (recepciones_tienda2 = sucursales.sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3) AND idRecepcion NOT IN(";
            
            for(int a = 0; a < listFirstJoin.size(); a++)
            {
                sqlSubQuery1.append(String.valueOf(listFirstJoin.get(a).getRecepcion().getId()));
                sqlSubQuery1.append(",");
            }
            
            sqlSubQuery1.deleteCharAt(sqlSubQuery1.lastIndexOf(",")); 
            sqlQuery += sqlSubQuery1.toString();
            sqlQuery += ")"; 
            sqlQuery += "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            ") " +
            "GROUP BY idRecepcion " +
            ") " +
            "GROUP BY idVenta " +
            ")";
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, idFolio);
            preparedStatement.setInt(2, idFolio);
            preparedStatement.setInt(3, idFolio);
            preparedStatement.setInt(4, idFolio);
            preparedStatement.setInt(5, idFolio);
            preparedStatement.setInt(6, idFolio);
            preparedStatement.setInt(7, idFolio);
            preparedStatement.setInt(8, idFolio);
            preparedStatement.setInt(9, idFolio);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next())
            {
                sale = new Sale();
                sale.setId(resultSet.getInt("id"));
                sale.setFolio(resultSet.getString("idFolio"));
                sale.setFecha(resultSet.getString("fecha"));
                sale.setPedidoAdicional(resultSet.getString("pedido_adicional"));
                sale.setFactura(resultSet.getString("factura"));
                sale.setFolio(resultSet.getString("folio"));
                sale.setSolicitante(resultSet.getString("solicitante"));
                sale.setCedis(resultSet.getString("cedis"));
                sale.setDestinatario(resultSet.getString("destinatario"));
                sale.setNombreDestinatario(resultSet.getString("nombre_destinatario"));
                sale.setFacturaRemisionSicav(resultSet.getString("factura_remisionSicav"));
                sale.setImporte(resultSet.getString("importe"));
                sale.setCliente(resultSet.getString("cliente"));
                sale.setRefFact(resultSet.getString("ref_fact"));
                sale.setReferencia(resultSet.getString("referencia"));
                sale.setClvRef2(resultSet.getString("clv_ref2"));
                sale.setClvRef3(resultSet.getString("clv_ref3"));
                sale.setFechaDoc(resultSet.getString("fecha_doc"));
                sale.setVencNeto(resultSet.getString("venc_neto"));
                sale.setImpteMl(resultSet.getString("impteML"));
                sale.setCe(resultSet.getString("ce"));
                sale.setDiv(resultSet.getString("div"));
                listSaleResult.add(sale);
            }
            resultSet.close();
            preparedStatement.close();
            //connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            listSaleResult = new ArrayList<Sale>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listSaleResult;
    }
    
    public ArrayList<Reception> getReceptionsNotMatch(IJidokaServer<?> server, String url, int idFolio, ArrayList<Link> listFirstJoin)
    {
        ArrayList<Reception> listReceptionResult = new ArrayList<Reception>(); 
        try
        {
            Reception reception = new Reception();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            StringBuilder sqlSubQuery1 = new StringBuilder();
            String sqlQuery = null;
            /*sqlQuery = "SELECT * FROM recepciones WHERE idFolio = ? AND id NOT IN(SELECT idRecepcion FROM (SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            //"GROUP BY idRecepcion " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            //"GROUP BY idRecepcion " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_adicional = ventas_pedido_adicional) " +
            //"GROUP BY idRecepcion " +
            "ORDER BY ventas_destinatario) " +
            "ORDER BY idRecepcion);";*/
            
            sqlQuery = "SELECT * FROM recepciones WHERE idFolio = ? AND id NOT IN(SELECT idRecepcion FROM(SELECT * FROM(SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " + 
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " + 
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND ((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion,  " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) AND (recepciones_adicional = ventas_pedido_adicional) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION " +
            "SELECT * FROM(SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas.idFolio = ?) AND (recepciones.idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3)) AND NOT(recepciones_adicional = ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (ventas_nombre_destinatario2 = sucursales.sucursal_LALA2) AND (recepciones_tienda2 = sucursales.sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3) AND idRecepcion NOT IN(";
            
            for(int a = 0; a < listFirstJoin.size(); a++)
            {
                sqlSubQuery1.append(String.valueOf(listFirstJoin.get(a).getRecepcion().getId()));
                sqlSubQuery1.append(",");
            }
            
            sqlSubQuery1.deleteCharAt(sqlSubQuery1.lastIndexOf(",")); 
            sqlQuery += sqlSubQuery1.toString();
            sqlQuery += ")";
            sqlQuery += "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            ") " +
            "GROUP BY idRecepcion " +
            ") " +
            "GROUP BY idVenta " +
            ")"; 
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, idFolio);
            preparedStatement.setInt(2, idFolio);
            preparedStatement.setInt(3, idFolio);
            preparedStatement.setInt(4, idFolio);
            preparedStatement.setInt(5, idFolio);
            preparedStatement.setInt(6, idFolio);
            preparedStatement.setInt(7, idFolio);
            preparedStatement.setInt(8, idFolio);
            preparedStatement.setInt(9, idFolio);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next())
            {
                reception = new Reception();
                reception.setId(resultSet.getInt("id"));
                reception.setIdFolio(resultSet.getInt("idFolio"));
                reception.setMtvo(resultSet.getString("mvto"));
                reception.setTienda(resultSet.getString("tienda"));
                reception.setRecibo(resultSet.getString("recibo"));
                reception.setOrden(resultSet.getString("orden"));
                reception.setAdicional(resultSet.getString("adicional"));
                reception.setRemision(resultSet.getString("remision"));
                reception.setFecha(resultSet.getString("fecha"));
                reception.setValor(resultSet.getString("valor"));
                reception.setIva(resultSet.getString("iva"));
                reception.setNeto(resultSet.getString("neto"));
                listReceptionResult.add(reception);
            }
            resultSet.close();
            preparedStatement.close();
            //connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            listReceptionResult = new ArrayList<Reception>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listReceptionResult;
    }
    
    public ArrayList<Link> searchLastWeek(IJidokaServer<?> server, String url, int currentIdFolio, int pastIdFolio)
    {
        ArrayList<Link> listResult = new ArrayList<Link>(); 
        try
        {
            int index = 0; 
            int times = 0; 
            Reception reception = new Reception();
            Sale sale = new Sale();
            Link link = new Link();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            String sqlQuery = "";
            String sqlSubQuery1 = ""; 
            String sqlSubQuery2 = "";
            sqlSubQuery1 = "SELECT * FROM(SELECT ventas_id, ventas_fecha, ventas_pedido_adicional, ventas_factura, ventas_folio, ventas_solicitante, ventas_cedis, ventas_destinatario, ventas_nombre_destinatario, ventas_abreviacion_factura, ventas_remisionSicav, ventas_importe, recepciones_id, recepciones_adicional, recepciones_tienda, recepciones_remision, recepciones_fecha, recepciones_neto, ROUND((ventas_importe - recepciones_neto), 2) AS amarre_diferencia, CAST(ROUND((ABS(ROUND((ventas_importe - recepciones_neto), 2)) / ventas_importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje  FROM(";
            sqlSubQuery1 += "SELECT * FROM (SELECT ventas_id, ventas_idFolio, ventas_fecha, ventas_pedido_adicional, ventas_factura, ventas_folio, ventas_solicitante, ventas_cedis, ventas_destinatario, ventas_nombre_destinatario, ventas_nombre_destinatario2, ventas_abreviacion_factura, ventas_remisionSicav, ventas_importe, (ventas_importe - ROUND(ventas_importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, (ventas_importe + ROUND(ventas_importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3 FROM (SELECT ventas.id AS ventas_id, ventas.idFolio AS ventas_idFolio, ventas.fecha AS ventas_fecha, ventas.pedido_adicional AS ventas_pedido_adicional, ventas.factura AS ventas_factura, ventas.folio AS ventas_folio, ventas.solicitante AS ventas_solicitante, ventas.cedis AS ventas_cedis, ventas.destinatario AS ventas_destinatario, ventas.nombre_destinatario AS ventas_nombre_destinatario, ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, substr(ventas.factura_remisionSicav, 0, 4) AS ventas_abreviacion_factura, substr(ventas.factura_remisionSicav, 4, length(ventas.factura_remisionSicav)) AS ventas_remisionSicav, ventas.importe AS ventas_importe FROM ventas WHERE idFolio = ? AND id NOT IN(SELECT idVenta FROM(SELECT * FROM(SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND ((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion,  " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) AND (recepciones_adicional = ventas_pedido_adicional) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION " +
            "SELECT * FROM(SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas.idFolio = ?) AND (recepciones.idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3)) AND NOT(recepciones_adicional = ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (ventas_nombre_destinatario2 = sucursal_LALA2) AND (recepciones_tienda2 = sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            ") " +
            "GROUP BY idRecepcion " +
            ") " +
            "GROUP BY idVenta " +
            ") " +
            ") " +
            "INNER JOIN documento ON documento.id = ventas_idFolio " +
            ")";
            sqlQuery += sqlSubQuery1;
            sqlQuery += "CROSS JOIN";
            
            index = sqlSubQuery1.indexOf("?");
            while(index >= 0) {
                index = sqlSubQuery1.indexOf("?", index+1);
                times++;
            }
            
            sqlSubQuery2 = "(SELECT recepciones_id, recepciones_adicional, recepciones_tienda, recepciones_tienda2, recepciones_remision, recepciones_fecha, recepciones_valor, recepciones_neto FROM(SELECT recepciones.id AS recepciones_id, recepciones.idFolio AS idFolio, recepciones.adicional AS recepciones_adicional, recepciones.tienda AS recepciones_tienda, recepciones.tienda2 AS recepciones_tienda2, recepciones.remision AS recepciones_remision, recepciones.fecha AS recepciones_fecha, recepciones.valor AS recepciones_valor, recepciones.neto AS recepciones_neto FROM recepciones WHERE idFolio = ? AND id NOT IN(SELECT idRecepcion FROM(SELECT * FROM(SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND ((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION  " +
            "SELECT * FROM(SELECT  " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion,  " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones INNER JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "INNER JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas_idFolio = ?) AND (recepciones_idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (((recepciones_valor = ventas_importe) OR (recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)))) AND (recepciones_adicional = ventas_pedido_adicional) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            "UNION " +
            "SELECT * FROM(SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio  " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "JOIN sucursales ON sucursales.sucursal_LALA2 = ventas.nombre_destinatario2 AND sucursales.sucursal_OXXO2 = recepciones.tienda2 " +
            "WHERE (ventas.idFolio = ?) AND (recepciones.idFolio = ?) AND NOT(recepciones_remision = ventas_remisionSicav) AND NOT((ventas_remisionSicav LIKE '%'|| recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3)) AND NOT(recepciones_adicional = ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (ventas_nombre_destinatario2 = sucursal_LALA2) AND (recepciones_tienda2 = sucursal_OXXO2) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3) " +
            "GROUP BY idRecepcion) " +
            "GROUP BY idVenta " +
            ") " +
            "GROUP BY idRecepcion " +
            ") " +
            "GROUP BY idVenta " +
            ") " +
            ") " +
            "JOIN documento ON documento.id = idFolio " +
            ")" +
            "LEFT JOIN sucursales ON (sucursales.sucursal_LALA2 = ventas_nombre_destinatario2)";
            
            index = sqlSubQuery2.indexOf("?");
            while(index >= 0) {
                index = sqlSubQuery2.indexOf("?", index+1);
                times++;
            }
            
            sqlQuery += sqlSubQuery2;
            sqlQuery += "WHERE (recepciones_remision = ventas_remisionSicav) OR (recepciones_adicional = ventas_pedido_adicional) OR (((ventas_nombre_destinatario2 = sucursal_LALA2)) AND ((recepciones_tienda2 = sucursal_OXXO2)) AND (recepciones_valor BETWEEN ventas_importe_2 AND ventas_importe_3))";
            sqlQuery += "GROUP BY ventas_id ) ";
            sqlQuery += "GROUP BY recepciones_id )"; 
            sqlQuery += "ORDER BY ventas_destinatario";
            
            /*sqlSubQuery1 = "SELECT ventas_fecha, ventas_pedido_adicional, ventas_factura, ventas_folio, ventas_solicitante, ventas_cedis, ventas_destinatario, ventas_nombre_destinatario, ventas_abreviacion_factura, ventas_remisionSicav, ventas_importe, recepciones_id, recepciones_adicional, recepciones_tienda, recepciones_remision, recepciones_fecha, recepciones_neto, ROUND((ventas_importe - recepciones_neto), 2) AS amarre_diferencia, CAST(ROUND((ABS(ROUND((ventas_importe - recepciones_neto), 2)) / ventas_importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje  FROM("; 
            sqlSubQuery1 += "SELECT * FROM (SELECT ventas_id, ventas_idFolio, ventas_fecha, ventas_pedido_adicional, ventas_factura, ventas_folio, ventas_solicitante, ventas_cedis, ventas_destinatario, ventas_nombre_destinatario, ventas_nombre_destinatario2, ventas_abreviacion_factura, ventas_remisionSicav, ventas_importe, (ventas_importe - ROUND(ventas_importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, (ventas_importe + ROUND(ventas_importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3 FROM (SELECT ventas.id AS ventas_id, ventas.idFolio AS ventas_idFolio, ventas.fecha AS ventas_fecha, ventas.pedido_adicional AS ventas_pedido_adicional, ventas.factura AS ventas_factura, ventas.folio AS ventas_folio, ventas.solicitante AS ventas_solicitante, ventas.cedis AS ventas_cedis, ventas.destinatario AS ventas_destinatario, ventas.nombre_destinatario AS ventas_nombre_destinatario, ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, substr(ventas.factura_remisionSicav, 0, 4) AS ventas_abreviacion_factura, substr(ventas.factura_remisionSicav, 4, length(ventas.factura_remisionSicav)) AS ventas_remisionSicav, ventas.importe AS ventas_importe FROM ventas WHERE idFolio = ? AND id NOT IN(SELECT idVenta FROM (SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            //"GROUP BY idVenta " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            //"GROUP BY idVenta " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_adicional = ventas_pedido_adicional) " +
            //"GROUP BY idVenta " +
            "ORDER BY ventas_destinatario) " +
            "ORDER BY idVenta) " +
            ") " +
            "JOIN documento ON documento.id = ventas_idFolio)";
            sqlQuery += sqlSubQuery1; 
            sqlQuery += "CROSS JOIN";
            
            index = sqlSubQuery1.indexOf("?");
            while(index >= 0) {
                index = sqlSubQuery1.indexOf("?", index+1);
                times++;
            }
            
            
            sqlSubQuery2 = "(SELECT recepciones_id, recepciones_adicional, recepciones_tienda, recepciones_tienda2, recepciones_remision, recepciones_fecha, recepciones_valor, recepciones_neto FROM (SELECT recepciones.id AS recepciones_id, recepciones.idFolio AS idFolio, recepciones.adicional AS recepciones_adicional, recepciones.tienda AS recepciones_tienda, recepciones.tienda2 AS recepciones_tienda2, recepciones.remision AS recepciones_remision, recepciones.fecha AS recepciones_fecha, recepciones.valor AS recepciones_valor, recepciones.neto AS recepciones_neto  FROM recepciones WHERE idFolio = ? AND id NOT IN(SELECT idRecepcion FROM (SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            //"GROUP BY idRecepcion " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            //"GROUP BY idRecepcion " +
            "UNION " +
            "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (ventas.idFolio = ?) AND (recepciones_adicional = ventas_pedido_adicional) " +
            //"GROUP BY idRecepcion " +
            "ORDER BY ventas_destinatario) " +
            "ORDER BY idRecepcion) " +
            ") " +
            "JOIN documento ON documento.id = idFolio)";
            
            index = sqlSubQuery2.indexOf("?");
            while(index >= 0) {
                index = sqlSubQuery2.indexOf("?", index+1);
                times++;
            }
            
            sqlQuery += sqlSubQuery2;
            sqlQuery += "WHERE (ventas_remisionSicav = recepciones_remision) OR (ventas_pedido_adicional = recepciones_adicional) OR ((ventas_nombre_destinatario2 = recepciones_tienda2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3))) " +
            //"GROUP BY ventas_id " +
            "ORDER BY ventas_destinatario);";*/
            
            
            preparedStatement = connection.prepareStatement(sqlQuery);
            for(int a = 1; a <= times; a++)
            {
                if(a > 0 && a <= 9)
                {
                    preparedStatement.setInt(a, currentIdFolio);
                }
                else if(a > 9 && a <= 18)
                {
                    preparedStatement.setInt(a, pastIdFolio);
                }
            }
            
            ResultSet resultSet = preparedStatement.executeQuery();
           
            while(resultSet.next())
            {
                link = new Link(); 
                reception = new Reception();
                sale = new Sale();
                sale.setId(resultSet.getInt("ventas_id"));
                sale.setFecha(resultSet.getString("ventas_fecha"));
                sale.setPedidoAdicional(resultSet.getString("ventas_pedido_adicional"));
                sale.setFactura(resultSet.getString("ventas_factura"));
                sale.setFolio(resultSet.getString("ventas_folio"));
                sale.setSolicitante(resultSet.getString("ventas_solicitante"));
                sale.setCedis(resultSet.getString("ventas_cedis"));
                sale.setDestinatario(resultSet.getString("ventas_destinatario"));
                sale.setNombreDestinatario(resultSet.getString("ventas_nombre_destinatario"));
                sale.setFacturaRemisionSicav(resultSet.getString("ventas_remisionSicav"));
                sale.setImporte(resultSet.getString("ventas_importe"));
                reception.setId(resultSet.getInt("recepciones_id"));
                reception.setAdicional(resultSet.getString("recepciones_adicional"));
                reception.setTienda(resultSet.getString("recepciones_tienda"));
                reception.setRemision(resultSet.getString("recepciones_remision"));
                reception.setFecha(resultSet.getString("recepciones_fecha"));
                reception.setNeto(resultSet.getString("recepciones_neto"));
                link.setAbreviacionVenta(resultSet.getString("ventas_abreviacion_factura"));
                link.setDiferencia(resultSet.getString("amarre_diferencia"));
                link.setPorcentaje(resultSet.getString("amarre_porcentaje"));
                link.setBusqueda("Folio Semana Pasada");
                link.setRecepcion(reception);
                link.setVenta(sale);
                listResult.add(link);
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
        }
        catch(SQLException e)
        {
            listResult = new ArrayList<Link>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listResult;
    }
    
    public ArrayList<Link> crossVirtualTable(IJidokaServer<?> server, String url, int firstIdFolio, int secondIdFolio)
    {
        ArrayList<Link> listResult = new ArrayList<Link>(); 
        try
        {
            int index = 0; 
            int times = 0; 
            Link link = new Link();
            Reception reception = new Reception();
            Sale sale = new Sale();
            Document document = new Document();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            String sqlQuery = "";
            String sqlSubQuery1 = ""; 
            String sqlSubQuery2 = "";
            sqlSubQuery1 = "SELECT * FROM(";
            sqlSubQuery1 += "SELECT  " +
            "recepciones.id AS recepciones_id, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.mvto AS recepciones_mvto, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.recibo AS recepciones_recibo, " +
            "recepciones.orden AS recepciones_orden, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.fecha2 AS recepciones_fecha2, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.iva AS recepciones_iva, " +
            "recepciones.neto AS recepciones_neto, " +
            "documento.id AS documento_id, " +
            "documento.nombre AS documento_nombre, " +
            "documento.numero_folio AS documento_numero_folio, " +
            "documento.porcentaje_incidencia AS documento_porcentaje_incidencia " +
            "FROM recepciones " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE recepciones.idFolio = ?)";
            sqlQuery += sqlSubQuery1;
            
            sqlQuery += "CROSS JOIN";
            
            sqlSubQuery2 = "(SELECT  " +
            "ventas.id AS ventas_id, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.fecha2 AS ventas_fecha2, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "ventas.factura_remisionSicav AS ventas_factura_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "ventas.cliente AS ventas_cliente, " +
            "ventas.ref_fact AS ventas_ref_fact, " +
            "ventas.referencia AS ventas_referencia, " +
            "ventas.clv_ref2 AS ventas_clv_ref2, " +
            "ventas.clv_ref3 AS ventas_clv_ref3, " +
            "ventas.fecha_doc AS ventas_fecha_doc, " +
            "ventas.fecha_doc2 AS ventas_fecha_doc2, " +
            "ventas.venc_neto AS ventas_venc_neto, " +
            "ventas.venc_neto2 AS ventas_venc_neto2, " +
            "ventas.impteML AS ventas_impteML, " +
            "ventas.ce AS ventas_ce, " +
            "ventas.div AS ventas_div " +
            "FROM ventas " +
            "WHERE ventas.idFolio = ?)";
            
            sqlQuery += sqlSubQuery2;
            
            
            
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, firstIdFolio);
            preparedStatement.setInt(2, secondIdFolio);
                
            
            ResultSet resultSet = preparedStatement.executeQuery();
           
            while(resultSet.next())
            {
                link = new Link(); 
                document = new Document();
                reception = new Reception();
                sale = new Sale();
                sale.setId(resultSet.getInt("ventas_id"));
                sale.setIdFolio(resultSet.getInt("ventas_idFolio"));
                sale.setFecha(resultSet.getString("ventas_fecha"));
                sale.setFecha2(resultSet.getString("ventas_fecha2"));
                sale.setPedidoAdicional(resultSet.getString("ventas_pedido_adicional"));
                sale.setFactura(resultSet.getString("ventas_factura"));
                sale.setFolio(resultSet.getString("ventas_folio"));
                sale.setSolicitante(resultSet.getString("ventas_solicitante"));
                sale.setCedis(resultSet.getString("ventas_cedis"));
                sale.setDestinatario(resultSet.getString("ventas_destinatario"));
                sale.setNombreDestinatario(resultSet.getString("ventas_nombre_destinatario"));
                sale.setNombreDestinatario2(resultSet.getString("ventas_nombre_destinatario2"));
                sale.setFacturaRemisionSicav(resultSet.getString("ventas_factura_remisionSicav"));
                sale.setImporte(resultSet.getString("ventas_importe"));
                sale.setCliente(resultSet.getString("ventas_cliente"));
                sale.setRefFact(resultSet.getString("ventas_ref_fact"));
                sale.setReferencia(resultSet.getString("ventas_referencia"));
                sale.setClvRef2(resultSet.getString("ventas_clv_ref2"));
                sale.setClvRef3(resultSet.getString("ventas_clv_ref3"));
                sale.setFechaDoc(resultSet.getString("ventas_fecha_doc"));
                sale.setFechaDoc2(resultSet.getString("ventas_fecha_doc2"));
                sale.setVencNeto(resultSet.getString("ventas_venc_neto"));
                sale.setVencNeto2(resultSet.getString("ventas_venc_neto2"));
                sale.setImpteMl(resultSet.getString("ventas_impteML"));
                sale.setCe(resultSet.getString("ventas_ce"));
                sale.setDiv(resultSet.getString("ventas_div"));
                
                reception.setId(resultSet.getInt("recepciones_id"));
                reception.setIdFolio(resultSet.getInt("recepciones_idFolio"));
                reception.setMtvo(resultSet.getString("recepciones_mvto"));
                reception.setTienda(resultSet.getString("recepciones_tienda"));
                reception.setTienda2(resultSet.getString("recepciones_tienda2"));
                reception.setRecibo(resultSet.getString("recepciones_recibo"));
                reception.setOrden(resultSet.getString("recepciones_orden"));
                reception.setAdicional(resultSet.getString("recepciones_adicional"));
                reception.setRemision(resultSet.getString("recepciones_remision"));
                reception.setFecha(resultSet.getString("recepciones_fecha"));
                reception.setFecha2(resultSet.getString("recepciones_fecha2"));
                reception.setValor(resultSet.getString("recepciones_valor"));
                reception.setIva(resultSet.getString("recepciones_iva"));
                reception.setNeto(resultSet.getString("recepciones_neto"));
                
                document.setId(resultSet.getInt("documento_id"));
                document.setNumeroFolio(resultSet.getString("documento_numero_folio"));
                document.setNombre(resultSet.getString("documento_nombre"));
                document.setPorcentajeIncidencia(resultSet.getString("documento_porcentaje_incidencia"));
                
                link.setDocumento(document);
                link.setVenta(sale);
                link.setRecepcion(reception);
                listResult.add(link);
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
        }
        catch(SQLException e)
        {
            listResult = new ArrayList<Link>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listResult;
    }
    
    public ArrayList<Reception> getFinalReceptionsNotMatch(IJidokaServer<?> server, String url, ArrayList listIdReception)
    {
        ArrayList<Reception> listReceptionResult = new ArrayList<Reception>(); 
        try
        {
            Reception reception = new Reception();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            String sqlQuery = null;
            StringBuilder sqlSubQuery1 = new StringBuilder();
            sqlQuery = "SELECT * FROM recepciones WHERE id IN(";
            for(int a = 0; a < listIdReception.size(); a++)
            {
                sqlSubQuery1.append(listIdReception.get(a).toString());
                sqlSubQuery1.append(",");
            }
            sqlSubQuery1.deleteCharAt(sqlSubQuery1.lastIndexOf(",")); 
            sqlQuery += sqlSubQuery1.toString();
            sqlQuery += ");"; 
            preparedStatement = connection.prepareStatement(sqlQuery);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next())
            {
                reception = new Reception();
                reception.setId(resultSet.getInt("id"));
                reception.setIdFolio(resultSet.getInt("idFolio"));
                reception.setMtvo(resultSet.getString("mvto"));
                reception.setTienda(resultSet.getString("tienda"));
                reception.setRecibo(resultSet.getString("recibo"));
                reception.setOrden(resultSet.getString("orden"));
                reception.setAdicional(resultSet.getString("adicional"));
                reception.setRemision(resultSet.getString("remision"));
                reception.setFecha(resultSet.getString("fecha"));
                reception.setValor(resultSet.getString("valor"));
                reception.setIva(resultSet.getString("iva"));
                reception.setNeto(resultSet.getString("neto"));
                listReceptionResult.add(reception);
            }
            resultSet.close();
            preparedStatement.close();
            //connection.commit();
            connection.close();
        }
        catch(SQLException e)
        {
            listReceptionResult = new ArrayList<Reception>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listReceptionResult;
    }
    
    public ArrayList<Link> getMatchSaleByCutDate(IJidokaServer<?> server, String url, int currentIdFolio, ArrayList listIdReception, ArrayList listIdSale)
    {
       ArrayList<Link> listResult = new ArrayList<Link>(); 
        try
        {
            int index = 0; 
            int times = 0; 
            Reception reception = new Reception();
            Sale sale = new Sale();
            Link link = new Link();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            String sqlQuery = "";
            StringBuilder sqlSubQuery1 = new StringBuilder(); 
            StringBuilder sqlSubQuery2 = new StringBuilder();
            sqlQuery += "SELECT * FROM(SELECT  " +
            "ventas_id, " +
            "ventas_idFolio, " +
            "ventas_fecha, " +
            "ventas_fecha2, " +
            "CAST((strftime('%d', ventas_fecha2)) AS INTEGER) AS ventas_fecha3, " +
            "ventas_pedido_adicional, " +
            "ventas_factura, " +
            "ventas_folio, " +
            "ventas_solicitante, " +
            "ventas_cedis, " +
            "ventas_destinatario, " +
            "ventas_nombre_destinatario, " +
            "ventas_nombre_destinatario2, " +
            "ventas_abreviacion_factura, " +
            "ventas_remisionSicav, " +
            "ventas_importe, " +
            "ventas_importe_2, " +
            "ventas_importe_3, " +
            "recepciones_id, " +
            "recepciones_idFolio, " +
            "recepciones_adicional, " +
            "recepciones_tienda, " +
            "recepciones_tienda2, " +
            "recepciones_remision, " +
            "recepciones_fecha, " +
            "recepciones_fecha2, " +
            "CAST(strftime('%d', recepciones_fecha2) AS INTEGER) AS recepciones_fecha3, " +
            "recepciones_valor, " +
            "recepciones_neto, " +
            "ROUND((ventas_importe - recepciones_neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas_importe - recepciones_neto), 2)) / ventas_importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje, " +
            "sucursales.sucursal_LALA2 AS sucursal_LALA2, " +
            "sucursales.sucursal_OXXO2 AS sucursal_OXXO2 " +
            "FROM (";
            sqlSubQuery1.append("SELECT * FROM(SELECT " +
            "ventas.id AS ventas_id, " +
            "ventas.idFolio AS ventas_idFolio, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.fecha2 AS ventas_fecha2, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "ventas.nombre_destinatario2 AS ventas_nombre_destinatario2, " +
            "substr(ventas.factura_remisionSicav, 0, 4) AS ventas_abreviacion_factura, " +
            "substr(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3 " +
            "FROM ventas  " +
            "INNER JOIN documento ON documento.id = ventas.idFolio " +
            "WHERE ventas.idFolio = ? AND ventas.id IN(");
            
            for(int a = 0; a < listIdSale.size(); a++)
            {
                sqlSubQuery1.append(listIdSale.get(a).toString());
                sqlSubQuery1.append(",");
            }
            sqlSubQuery1.deleteCharAt(sqlSubQuery1.lastIndexOf(","));
            sqlSubQuery1.append(")");
            sqlSubQuery1.append(")");
            
            sqlQuery += sqlSubQuery1.toString();
            
            sqlQuery += "CROSS JOIN";
            
            sqlSubQuery2.append("(SELECT  " +
            "recepciones.id AS recepciones_id, " +
            "recepciones.idFolio AS recepciones_idFolio, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "recepciones.tienda2 AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.fecha2 AS recepciones_fecha2, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto " +
            "FROM recepciones  " +
            "WHERE recepciones.idFolio = ? AND id IN(");
            
            for(int b = 0; b < listIdReception.size(); b++)
            {
                sqlSubQuery2.append(listIdReception.get(b).toString());
                sqlSubQuery2.append(","); 
            }
            sqlSubQuery2.deleteCharAt(sqlSubQuery2.lastIndexOf(","));
            sqlSubQuery2.append(")");
            sqlSubQuery2.append(")");
            
            sqlQuery += sqlSubQuery2.toString();
            
            sqlQuery += ")";
            sqlQuery += "LEFT JOIN sucursales ON sucursales.sucursal_LALA2 = ventas_nombre_destinatario2 ";
            sqlQuery += "WHERE (ventas_remisionSicav = recepciones_remision) OR (ventas_pedido_adicional = recepciones_adicional) OR ((ventas_remisionSicav LIKE '%' || recepciones_remision) AND (recepciones_tienda2 = sucursal_OXXO2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3))) OR ((recepciones_fecha3 <= ventas_fecha3) AND (ventas_nombre_destinatario2 = sucursal_LALA2) AND (recepciones_tienda2 = sucursal_OXXO2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3))) "; 
            sqlQuery += "GROUP BY recepciones_id ";
            sqlQuery += ") ";
            sqlQuery += "GROUP BY ventas_id ";
            sqlQuery += "ORDER BY ventas_destinatario";
            
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, currentIdFolio);
            preparedStatement.setInt(2, currentIdFolio);
                        
            ResultSet resultSet = preparedStatement.executeQuery();
           
            while(resultSet.next())
            {
                link = new Link(); 
                reception = new Reception();
                sale = new Sale();
                sale.setId(resultSet.getInt("ventas_id"));
                sale.setFecha(resultSet.getString("ventas_fecha"));
                sale.setPedidoAdicional(resultSet.getString("ventas_pedido_adicional"));
                sale.setFactura(resultSet.getString("ventas_factura"));
                sale.setFolio(resultSet.getString("ventas_folio"));
                sale.setSolicitante(resultSet.getString("ventas_solicitante"));
                sale.setCedis(resultSet.getString("ventas_cedis"));
                sale.setDestinatario(resultSet.getString("ventas_destinatario"));
                sale.setNombreDestinatario(resultSet.getString("ventas_nombre_destinatario"));
                sale.setFacturaRemisionSicav(resultSet.getString("ventas_remisionSicav"));
                sale.setImporte(resultSet.getString("ventas_importe"));
                reception.setId(resultSet.getInt("recepciones_id"));
                reception.setAdicional(resultSet.getString("recepciones_adicional"));
                reception.setTienda(resultSet.getString("recepciones_tienda"));
                reception.setRemision(resultSet.getString("recepciones_remision"));
                reception.setFecha(resultSet.getString("recepciones_fecha"));
                reception.setNeto(resultSet.getString("recepciones_neto"));
                link.setAbreviacionVenta(resultSet.getString("ventas_abreviacion_factura"));
                link.setDiferencia(resultSet.getString("amarre_diferencia"));
                link.setPorcentaje(resultSet.getString("amarre_porcentaje"));
                link.setBusqueda("Ventas Oxxo por Fecha de Corte");
                link.setRecepcion(reception);
                link.setVenta(sale);
                listResult.add(link);
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
        }
        catch(SQLException e)
        {
            listResult = new ArrayList<Link>();
            server.info(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            server.error(sw.toString());
        }
        return listResult; 
    }
    
    
    public void documentTableColumns()
    {
        documentColumns = null;
        documentColumns = new HashMap<Integer, String>();
        
        documentColumns.put(0,"id");
        documentColumns.put(1, "numero_folio");
        documentColumns.put(2, "nombre");
        documentColumns.put(3, "porcentaje_incidencia");
    }
    
    public void receptionTableColumns()
    {
        receptionsColumns = null;
        receptionsColumns = new HashMap<Integer, String>();
        
        receptionsColumns.put(0, "id");
        receptionsColumns.put(1, "idFolio");
        receptionsColumns.put(2, "mvto");
        receptionsColumns.put(3, "tienda");
        receptionsColumns.put(4, "tienda2");
        receptionsColumns.put(5, "recibo");
        receptionsColumns.put(6, "orden");
        receptionsColumns.put(7, "adicional");
        receptionsColumns.put(8, "remision");
        receptionsColumns.put(9, "fecha");
        receptionsColumns.put(10, "fecha2"); 
        receptionsColumns.put(11, "valor");
        receptionsColumns.put(12, "iva");
        receptionsColumns.put(13, "neto");
    }
    
    public void virtualReceptionTableColumns()
    {
        virtualReceptionsColumns = null;
        virtualReceptionsColumns = new HashMap<Integer, String>();
        
        virtualReceptionsColumns.put(0, "idFolio");
        virtualReceptionsColumns.put(1, "mvto");
        virtualReceptionsColumns.put(2, "tienda");
        virtualReceptionsColumns.put(3, "tienda2");
        virtualReceptionsColumns.put(4, "recibo");
        virtualReceptionsColumns.put(5, "orden");
        virtualReceptionsColumns.put(6, "adicional");
        virtualReceptionsColumns.put(7, "remision");
        virtualReceptionsColumns.put(8, "fecha");
        virtualReceptionsColumns.put(9, "fecha2");
        virtualReceptionsColumns.put(10, "valor");
        virtualReceptionsColumns.put(11, "iva");
        virtualReceptionsColumns.put(12, "neto");
    }
    
    public void saleTableColumns()
    {
        salesColumns = null;
        salesColumns = new HashMap<Integer, String>();
        
        salesColumns.put(0, "id");
        salesColumns.put(1, "idFolio");
        salesColumns.put(2, "fecha");
        salesColumns.put(3, "fecha2");
        salesColumns.put(4, "pedido_adicional");
        salesColumns.put(5, "factura");
        salesColumns.put(6, "folio");
        salesColumns.put(7, "solicitante");
        salesColumns.put(8, "cedis");
        salesColumns.put(9, "destinatario");
        salesColumns.put(10, "nombre_destinatario");
        salesColumns.put(11, "nombre_destinatario2");
        salesColumns.put(12, "factura_remisionSicav");
        salesColumns.put(13, "importe");
        salesColumns.put(14, "cliente");
        salesColumns.put(15, "ref_fact");
        salesColumns.put(16, "referencia");
        salesColumns.put(17, "clv_ref2");
        salesColumns.put(18, "clv_ref3");
        salesColumns.put(19, "fecha_doc");
        salesColumns.put(20, "fecha_doc2");
        salesColumns.put(21, "venc_neto");
        salesColumns.put(22, "venc_neto2");
        salesColumns.put(23, "impteML");
        salesColumns.put(24, "ce");
        salesColumns.put(25, "div");
    }
    
    public void branchTableColumns()
    {
        branchColumns = null;
        branchColumns = new HashMap<Integer, String>();
        
        branchColumns.put(0, "id");
        branchColumns.put(1, "analista");
        branchColumns.put(2, "cliente");
        branchColumns.put(3, "zona");
        branchColumns.put(4, "centro");
        branchColumns.put(5, "cedis");
        branchColumns.put(6, "sucursal_SAP");
        branchColumns.put(7, "sucursal_LALA");
        branchColumns.put(8, "sucursal_LALA2");
        branchColumns.put(9, "cr_LALA");
        branchColumns.put(10, "plaza1");
        branchColumns.put(11, "cr_OXXO");
        branchColumns.put(12, "plaza2");
        branchColumns.put(13, "sucursal_OXXO");
        branchColumns.put(14, "sucursal_OXXO2");
        branchColumns.put(15, "liquidacion");
        branchColumns.put(16, "venta_cruzada");
    }
    
    public void virtualSaleTableColumns()
    {
        virtualSalesColumns = null;
        virtualSalesColumns = new HashMap<Integer, String>();
        
        virtualSalesColumns.put(0, "idFolio");
        virtualSalesColumns.put(1, "fecha");
        virtualSalesColumns.put(2, "fecha2");
        virtualSalesColumns.put(3, "pedido_adicional");
        virtualSalesColumns.put(4, "factura");
        virtualSalesColumns.put(5, "folio");
        virtualSalesColumns.put(6, "solicitante");
        virtualSalesColumns.put(7, "cedis");
        virtualSalesColumns.put(8, "destinatario");
        virtualSalesColumns.put(9, "nombre_destinatario");
        virtualSalesColumns.put(10, "nombre_destinatario2");
        virtualSalesColumns.put(11, "factura_remisionSicav");
        virtualSalesColumns.put(12, "importe");
        virtualSalesColumns.put(13, "cliente");
        virtualSalesColumns.put(14, "ref_fact");
        virtualSalesColumns.put(15, "referencia");
        virtualSalesColumns.put(16, "clv_ref2");
        virtualSalesColumns.put(17, "clv_ref3");
        virtualSalesColumns.put(18, "fecha_doc");
        virtualSalesColumns.put(19, "fecha_doc2");
        virtualSalesColumns.put(20, "venc_neto");
        virtualSalesColumns.put(21, "venc_neto2");
        virtualSalesColumns.put(22, "impteML");
        virtualSalesColumns.put(23, "ce");
        virtualSalesColumns.put(24, "div");
    }
    
    public void virtualTableColumns()
    {
        virtualTableColumns = null;
        virtualTableColumns = new HashMap<Integer, String>();
        
        virtualTableColumns.put(0, "recepciones_id");
        virtualTableColumns.put(1, "recepciones_idFolio");
        virtualTableColumns.put(2, "recepciones_mvto");
        virtualTableColumns.put(3, "recepciones_tienda");
        virtualTableColumns.put(4, "recepciones_tienda2");
        virtualTableColumns.put(5, "recepciones_recibo");
        virtualTableColumns.put(6, "recepciones_orden");
        virtualTableColumns.put(7, "recepciones_adicional");
        virtualTableColumns.put(8, "recepciones_remision");
        virtualTableColumns.put(9, "recepciones_fecha");
        virtualTableColumns.put(10, "recepciones_fecha2");
        virtualTableColumns.put(11, "recepciones_valor");
        virtualTableColumns.put(12, "recepciones_iva");
        virtualTableColumns.put(13, "recepciones_neto");
        virtualTableColumns.put(14, "ventas_id");
        virtualTableColumns.put(15, "ventas_idFolio");
        virtualTableColumns.put(16, "ventas_fecha");
        virtualTableColumns.put(17, "ventas_fecha2");
        virtualTableColumns.put(18, "ventas_pedido_adicional");
        virtualTableColumns.put(19, "ventas_factura");
        virtualTableColumns.put(20, "ventas_folio");
        virtualTableColumns.put(21, "ventas_solicitante");
        virtualTableColumns.put(22, "ventas_cedis");
        virtualTableColumns.put(23, "ventas_destinatario");
        virtualTableColumns.put(24, "ventas_nombre_destinatario");
        virtualTableColumns.put(25, "ventas_nombre_destinatario2");
        virtualTableColumns.put(26, "ventas_factura_remisionSicav");
        virtualTableColumns.put(27, "ventas_importe");
        virtualTableColumns.put(28, "ventas_cliente");
        virtualTableColumns.put(29, "ventas_ref_fact");
        virtualTableColumns.put(30, "ventas_referencia");
        virtualTableColumns.put(31, "ventas_clv_ref2");
        virtualTableColumns.put(32, "ventas_clv_ref3");
        virtualTableColumns.put(33, "ventas_fecha_doc");
        virtualTableColumns.put(34, "ventas_fecha_doc2");
        virtualTableColumns.put(35, "ventas_venc_neto");
        virtualTableColumns.put(36, "ventas_venc_neto2");
        virtualTableColumns.put(37, "ventas_impteML");
        virtualTableColumns.put(38, "ventas_ce");
        virtualTableColumns.put(39, "ventas_div");
        virtualTableColumns.put(40, "documento_id");
        virtualTableColumns.put(41, "documento_nombre");
        virtualTableColumns.put(42, "documento_numero_folio");
        virtualTableColumns.put(43, "documento_porcentaje_incidencia");
    }
}
