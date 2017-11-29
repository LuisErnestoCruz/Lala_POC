/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.database;

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
    private Map<Integer, String> salesColumns;

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
                        case 9: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        case 10: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        case 11: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        default: columnParameters += " TEXT NOT NULL,"; break; 
                    }
                }
                columnParameters += " FOREIGN KEY(`idFolio`) REFERENCES `documento`(`id`)";
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
                prepareStatement.setString(4, reception.getRecibo());
                prepareStatement.setString(5, reception.getOrden());
                prepareStatement.setString(6, reception.getAdicional());
                prepareStatement.setString(7, reception.getRemision());
                prepareStatement.setString(8, reception.getFecha());
                prepareStatement.setString(9, reception.getValor());
                prepareStatement.setString(10, reception.getIva());
                prepareStatement.setString(11, reception.getNeto());
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
                prepareStatement.setString(3, sale.getPedidoAdicional());
                prepareStatement.setString(4, sale.getFactura());
                prepareStatement.setString(5, sale.getFolio());
                prepareStatement.setString(6, sale.getSolicitante());
                prepareStatement.setString(7, sale.getCedis());
                prepareStatement.setString(8, sale.getDestinatario());
                prepareStatement.setString(9, sale.getNombreDestinatario());
                prepareStatement.setString(10, sale.getFacturaRemisionSicav());
                prepareStatement.setDouble(11, Double.parseDouble(sale.getImporte()));
                prepareStatement.setString(12, sale.getCliente());
                prepareStatement.setString(13, sale.getRefFact());
                prepareStatement.setString(14, sale.getReferencia());
                prepareStatement.setString(15, sale.getClvRef2());
                prepareStatement.setString(16, sale.getClvRef3());
                prepareStatement.setString(17, sale.getFechaDoc());
                prepareStatement.setString(18, sale.getVencNeto());
                if(StringUtils.isBlank(sale.getImpteMl())) { prepareStatement.setString(19, null);} else { prepareStatement.setDouble(19, Double.parseDouble(sale.getImpteMl())); }
                prepareStatement.setString(20, sale.getCe());
                prepareStatement.setString(21, sale.getDiv());
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
                        case 11: receptionInformation += "?"; break;
                        default: receptionInformation += "?"; break;
                        
                    }
                }
            }
        }
        return receptionInformation;
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
                        case 21: saleInformation += "?"; break;
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
                        case 11: columnParameters += " " + columns.get(a) + " REAL NOT NULL,"; break;
                        case 12: columnParameters += " " + columns.get(a) + " TEXT NOT NULL,"; break;
                        case 13: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 14: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 15: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 16: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 17: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 18: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 19: columnParameters += " " + columns.get(a) + " REAL,"; break;
                        case 20: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        case 21: columnParameters += " " + columns.get(a) + " TEXT,"; break;
                        default: columnParameters += " TEXT,"; break; 
                    }
                }
                columnParameters += " FOREIGN KEY(`idFolio`) REFERENCES `documento`(`id`)";
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
    
    public ArrayList<Link> searchByStoreDateAmount(IJidokaServer<?> server, String url, int idFolio)
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
            sqlQuery = "SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), '-')+1, length(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''))) AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_abreviacion_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "substr(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(recepciones.tienda), ' ', ''), '-')+1, length(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''))) AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto, " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            "GROUP BY idVenta " +
            "ORDER BY ventas_destinatario";
            server.info("Query:" + sqlQuery);
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, idFolio);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next())
            {
                link = new Link(); 
                reception = new Reception();
                sale = new Sale();
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
    
    public ArrayList<Sale> getSalesNotMatch(IJidokaServer<?> server, String url, int idFolio)
    {
        ArrayList<Sale> listSaleResult = new ArrayList<Sale>(); 
        try
        {
            Sale sale = new Sale();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            String sqlQuery = null;
            sqlQuery = "SELECT * FROM ventas WHERE idFolio = ? AND id NOT IN(SELECT idVenta FROM (SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''))) AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(recepciones.tienda), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''))) AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto, " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (ventas.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            "GROUP BY idVenta " +
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
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''))) AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(recepciones.tienda), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''))) AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (ventas.idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            "GROUP BY idVenta " +
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
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''))) AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(recepciones.tienda), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''))) AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (ventas.idFolio = ?) AND (recepciones_adicional = ventas_pedido_adicional) " +
            "GROUP BY idVenta " +
            "ORDER BY ventas_destinatario) " +
            "ORDER BY idVenta);";
            
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, idFolio);
            preparedStatement.setInt(2, idFolio);
            preparedStatement.setInt(3, idFolio);
            preparedStatement.setInt(4, idFolio);
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
    
    public ArrayList<Reception> getReceptionsNotMatch(IJidokaServer<?> server, String url, int idFolio)
    {
        ArrayList<Reception> listReceptionResult = new ArrayList<Reception>(); 
        try
        {
            Reception reception = new Reception();
            Connection connection = connectDatabase(server, url);
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement =  null;
            String sqlQuery = null;
            sqlQuery = "SELECT * FROM recepciones WHERE idFolio = ? AND id NOT IN(SELECT idRecepcion FROM (SELECT " +
            "ventas.id AS idVenta, " +
            "ventas.fecha AS ventas_fecha, " +
            "ventas.pedido_adicional AS ventas_pedido_adicional, " +
            "ventas.factura AS ventas_factura, " +
            "ventas.folio AS ventas_folio, " +
            "ventas.solicitante AS ventas_solicitante, " +
            "ventas.cedis AS ventas_cedis, " +
            "ventas.destinatario AS ventas_destinatario, " +
            "ventas.nombre_destinatario AS ventas_nombre_destinatario, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''))) AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(recepciones.tienda), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''))) AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (recepciones_remision <> ventas_remisionSicav) AND (recepciones_adicional <> ventas_pedido_adicional) AND (recepciones_fecha = ventas_fecha) AND (recepciones_tienda2 = ventas_nombre_destinatario2) AND ((recepciones_valor >= ventas_importe_2 AND recepciones_valor <= ventas_importe) OR (recepciones_valor >= ventas_importe AND recepciones_valor <= ventas_importe_3)) " +
            "GROUP BY idRecepcion " +
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
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''))) AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(recepciones.tienda), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''))) AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM ventas JOIN recepciones ON recepciones.idFolio = ventas.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (recepciones_remision = ventas_remisionSicav) " +
            "GROUP BY idRecepcion " +
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
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(ventas.nombre_destinatario), ' ', ''), ',', ''), '.', ''))) AS ventas_nombre_destinatario2, " +
            "SUBSTR(ventas.factura_remisionSicav, 0, 4) AS ventas_factura, " +
            "SUBSTR(ventas.factura_remisionSicav, 4, LENGTH(ventas.factura_remisionSicav)) AS ventas_remisionSicav, " +
            "ventas.importe AS ventas_importe, " +
            "(ventas.importe - ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_2, " +
            "(ventas.importe + ROUND(ventas.importe * documento.porcentaje_incidencia, 2)) AS ventas_importe_3, " +
            "recepciones.id AS idRecepcion, " +
            "recepciones.adicional AS recepciones_adicional, " +
            "recepciones.tienda AS recepciones_tienda, " +
            "SUBSTR(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''), INSTR(REPLACE(LOWER(recepciones.tienda), ' ', ''), '-')+1, LENGTH(REPLACE(REPLACE(REPLACE(LOWER(recepciones.tienda), ' ', ''), ',', ''), '.', ''))) AS recepciones_tienda2, " +
            "recepciones.remision AS recepciones_remision, " +
            "recepciones.fecha AS recepciones_fecha, " +
            "recepciones.valor AS recepciones_valor, " +
            "recepciones.neto AS recepciones_neto,  " +
            "ROUND((ventas.importe - recepciones.neto), 2) AS amarre_diferencia, " +
            "CAST(ROUND((ABS(ROUND((ventas.importe - recepciones.neto), 2)) / ventas.importe) * 100, 2) AS TEXT) || '%' AS amarre_porcentaje " +
            "FROM recepciones JOIN ventas ON ventas.idFolio = recepciones.idFolio " +
            "JOIN documento ON documento.id = recepciones.idFolio " +
            "WHERE (recepciones.idFolio = ?) AND (recepciones_adicional = ventas_pedido_adicional) " +
            "GROUP BY idRecepcion " +
            "ORDER BY ventas_destinatario) " +
            "ORDER BY idRecepcion);";
            
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setInt(1, idFolio);
            preparedStatement.setInt(2, idFolio);
            preparedStatement.setInt(3, idFolio);
            preparedStatement.setInt(4, idFolio);
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
        receptionsColumns.put(4, "recibo");
        receptionsColumns.put(5, "orden");
        receptionsColumns.put(6, "adicional");
        receptionsColumns.put(7, "remision");
        receptionsColumns.put(8, "fecha");
        receptionsColumns.put(9, "valor");
        receptionsColumns.put(10, "iva");
        receptionsColumns.put(11, "neto");
    }
    
    public void saleTableColumns()
    {
        salesColumns = null;
        salesColumns = new HashMap<Integer, String>();
        
        salesColumns.put(0, "id");
        salesColumns.put(1, "idFolio");
        salesColumns.put(2, "fecha");
        salesColumns.put(3, "pedido_adicional");
        salesColumns.put(4, "factura");
        salesColumns.put(5, "folio");
        salesColumns.put(6, "solicitante");
        salesColumns.put(7, "cedis");
        salesColumns.put(8, "destinatario");
        salesColumns.put(9, "nombre_destinatario");
        salesColumns.put(10, "factura_remisionSicav");
        salesColumns.put(11, "importe");
        salesColumns.put(12, "cliente");
        salesColumns.put(13, "ref_fact");
        salesColumns.put(14, "referencia");
        salesColumns.put(15, "clv_ref2");
        salesColumns.put(16, "clv_ref3");
        salesColumns.put(17, "fecha_doc");
        salesColumns.put(18, "venc_neto");
        salesColumns.put(19, "impteML");
        salesColumns.put(20, "ce");
        salesColumns.put(21, "div");
    }    
}
