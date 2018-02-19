/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.file;

import com.monitorjbl.xlsx.StreamingReader;
import com.novayre.jidoka.client.api.IJidokaServer;
import java.awt.Color;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 *
 * @author X220
 */
public class OutputExcelFile {
    
    public static void createExcelFile(IJidokaServer<?> server, String fileName)
    {
        try
        {
            StringBuilder excelFilePath = new StringBuilder(); 
            excelFilePath.append(server.getCurrentDir());
            excelFilePath.append("\\");
            excelFilePath.append(fileName);
            excelFilePath.append(".xlsx"); 
            //excelFilePath = excelFilePath.replace("\\", "\\\\");
            XSSFWorkbook workbook = new XSSFWorkbook();
            
            XSSFSheet sheet = workbook.createSheet("Folio");
            
            FileOutputStream outputStream = new FileOutputStream(excelFilePath.toString());
            workbook.write(outputStream);
            workbook.close();
        }
        catch(Exception er)
        {
            server.error(er.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            er.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static void createFolioSheet(IJidokaServer<?> server, String fileName, ArrayList<String> columnNames, ArrayList<Header> headers, ArrayList<Perception> perceptions)
    {
        try
        {
            int countRow = 0; 
            int countColumn = 0;
            Row row = null;
            Cell cell = null;
            CellStyle cellStyle = null;
            CellStyle totalStyle = null;
            CellStyle currencyStyle = null;
            CellStyle refundStyle = null;
            CellStyle totalMOVStyle = null;
            CellStyle totalColumnStyle = null;
            XSSFColor columnColor = null;
            XSSFColor refundColor = null;
            XSSFColor totalMOVColor = null;
            XSSFColor totalColor = null;
            XSSFFont font = null;
            FileOutputStream outputStream = null;
            String dateColumnContent = "";
            StringBuilder excelFilePath = new StringBuilder(); 
            excelFilePath.append(server.getCurrentDir());
            excelFilePath.append("\\");
            excelFilePath.append(fileName);
            excelFilePath.append(".xlsx");
            
            FileInputStream inputFile =new FileInputStream(new File(excelFilePath.toString()));
            XSSFWorkbook book = new XSSFWorkbook(inputFile);
            Sheet folioSheet = book.getSheetAt(0);
            
            refundColor = new XSSFColor(new Color(254, 0, 0));
            totalMOVColor = new XSSFColor(new Color(170, 0, 51));
            totalColor = new XSSFColor(new Color(221, 0, 51));
            cellStyle = book.createCellStyle();
            font = book.createFont();
            font.setBold(true);
            
            cellStyle.setFont(font);
            
            for(Header header: headers)
            {
                folioSheet.addMergedRegion(new CellRangeAddress(countRow, countRow, 0, 7));
                row = folioSheet.createRow(countRow);
                countRow = countRow + 1; 
                
                for(int a = 0; a < 3; a++)
                {
                    switch(a)
                    {
                        case 0:
                            cell = row.createCell(a);
                            cell.setCellValue(header.getColumn1()); 
                            cell.setCellStyle(cellStyle);
                            break;
                        case 1:  
                            cell = row.createCell(8);
                            cell.setCellValue(header.getColumn2());
                            cell.setCellStyle(cellStyle);
                            break;
                        case 2: 
                            cell = row.createCell(9);
                            cell.setCellValue(header.getColumn3());
                            cell.setCellStyle(cellStyle);
                            break;
                    }
                }
            }
            
            row = folioSheet.createRow(countRow);
            
            columnColor = new XSSFColor(new Color(119, 119, 170)); 
            
            cellStyle = book.createCellStyle();

            font = book.createFont();
            font.setColor(columnColor);
            font.setBold(true);
            font.setFontName("Arial");
            font.setFontHeightInPoints((short) 9);
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            for(int b = 0; b < columnNames.size(); b++)
            {
                cell = row.createCell(b); 
                cell.setCellValue(columnNames.get(b));
                cell.setCellStyle(cellStyle);
            }
            
            countRow = countRow + 1;
            
            row = folioSheet.createRow(countRow);
            
            cellStyle = book.createCellStyle();
            
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            totalColumnStyle = book.createCellStyle();
           
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            totalColumnStyle.setFont(font);
            totalColumnStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            currencyStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            currencyStyle.setDataFormat((short)8);
            currencyStyle.setFont(font);
            currencyStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            refundStyle = book.createCellStyle();
            refundStyle.setDataFormat((short)8);
            font = book.createFont();
            font.setColor(refundColor);
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            refundStyle.setFont(font);

            totalMOVStyle = book.createCellStyle();
            totalMOVStyle.setDataFormat((short)8);
            font = book.createFont();
            font.setColor(totalMOVColor);
            font.setBold(true);
            font.setFontName("Arial");
            font.setFontHeight(9);
            totalMOVStyle.setFont(font);
            totalMOVStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            totalStyle = book.createCellStyle();
            totalStyle.setDataFormat((short)8);
            font = book.createFont();
            font.setColor(totalColor);
            font.setBold(true);
            font.setFontName("Arial");
            font.setFontHeight(9);
            totalStyle.setFont(font);
            totalStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            for(Perception perception: perceptions)
            {
                row = folioSheet.createRow(countRow); 
                dateColumnContent = ""; 
                dateColumnContent = perception.getFecha();
                if(dateColumnContent != null)
                {
                    dateColumnContent = dateColumnContent.replace(" ", ""); 
                    dateColumnContent = dateColumnContent.toUpperCase();
                }
                else
                {
                    dateColumnContent = ""; 
                }
                
                for(int c = 0; c < columnNames.size(); c++)
                {
                   cell = row.createCell(c);
                   if(getPerceptionContent(c, perception).indexOf("$") >= 0)
                   {
                       if(StringUtils.containsAny("TOTAL", dateColumnContent) == false)
                       {
                            if(getPerceptionContent(c, perception).indexOf("-") >= 0)
                            {
                                 cell.setCellValue(getPerceptionContent(c, perception));
                                 cell.setCellStyle(refundStyle);
                            }
                            else 
                            {
                                cell.setCellValue(getPerceptionContent(c, perception));
                                cell.setCellStyle(currencyStyle);
                            }
                       }
                       else 
                       {
                           if(dateColumnContent.equals("TOTAL:"))
                           {
                               cell.setCellValue(getPerceptionContent(c, perception));
                               cell.setCellStyle(totalStyle);
                           }
                           else if(StringUtils.containsAny("TOTAL", dateColumnContent) && StringUtils.containsAny("MOV", dateColumnContent))
                           {
                               cell.setCellValue(getPerceptionContent(c, perception));
                               cell.setCellStyle(totalMOVStyle);
                           }
                       }
                   }
                   else
                   {
                       if(StringUtils.containsAny("TOTAL", getPerceptionContent(c, perception)))
                       {    
                            cell.setCellValue(getPerceptionContent(c, perception));
                            cell.setCellStyle(totalColumnStyle);
                            
                       }
                       else
                       {
                            cell.setCellValue(getPerceptionContent(c, perception));
                            cell.setCellStyle(cellStyle);
                       }
                   }
                   
                }
                
                if(StringUtils.containsAny("TOTAL", perception.getFecha().toUpperCase()))
                {
                    countRow = countRow + 1;
                    row = folioSheet.createRow(countRow);
                }
                
                countRow = countRow + 1;
            }
            
            outputStream = new FileOutputStream(excelFilePath.toString());
            book.write(outputStream);
            book.close();
        }
        catch(Exception er)
        {
            server.error(er.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            er.printStackTrace(pw);
            server.error(sw.toString());
        }
    }
    
    public static String getPerceptionContent(int position, Perception perception)
    {
        String value = "";
        switch(position)
        {
            case 0: value = perception.getMtvo(); break;
            case 1: value = perception.getTienda(); break;
            case 2: value = perception.getNumeroRecibo(); break;
            case 3: value = perception.getNumeroOrden(); break;
            case 4: value = perception.getNumeroPedidoAdicional(); break;
            case 5: value = perception.getNumeroRemision(); break;
            case 6: value = perception.getFecha(); break;
            case 7: value = perception.getValor(); break;
            case 8: value = perception.getIva(); break;
            case 9: value = perception.getNeto(); break; 
            default: break;
        }
        return value;
    }
}
