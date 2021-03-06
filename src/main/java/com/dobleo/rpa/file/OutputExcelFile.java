/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dobleo.rpa.file;

import com.dobleo.rpa.models.Folio;
import com.dobleo.rpa.models.Link;
import com.dobleo.rpa.models.Sale;
import com.monitorjbl.xlsx.StreamingReader;
import com.novayre.jidoka.client.api.IJidokaServer;
import java.awt.Color;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IgnoredErrorType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
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
    
    public static void createFolioNumberSheet(IJidokaServer<?> server, String fileName, ArrayList<String> columnNames, ArrayList<Header> headers, ArrayList<Perception> perceptions, ArrayList<Perception> cleanPerceptions)
    {
        try
        {
            boolean checkHeader = false;
            int countRow = 0; 
            int countColumn = 0;
            int firstRowIndex = 0;
            int lastRowIndex = 0;
            int lastFreezeRowIndex = 0; 
            double currencyContent = 0.0; 
            String folioType = null;
            Row row = null;
            Cell cell = null;
            CellStyle cellStyle = null;
            CellStyle cellDateStyle = null;
            CellStyle currencyStyle = null;
            CellStyle totalColumnStyle = null;
            CellStyle totalStyle = null;
            
            XSSFColor columnColor = null;
            XSSFColor totalColor = null;
            XSSFFont font = null;
            XSSFFont totalFont = null;
            
            CreationHelper createHelper = null; 
            FileOutputStream outputStream = null;
            Validation validation = null;
            String currencyColumnContent = ""; 
            NumberFormat currencyFormat = null; 
            SimpleDateFormat simpleDateFormat = null; 
            Date date = null; 
            StringBuilder excelFilePath = new StringBuilder(); 
            excelFilePath.append(server.getCurrentDir());
            excelFilePath.append("\\");
            excelFilePath.append(fileName);
            excelFilePath.append(".xlsx");
            
            FileInputStream inputFile =new FileInputStream(new File(excelFilePath.toString()));
            XSSFWorkbook book = new XSSFWorkbook(inputFile);
            XSSFSheet folioNumberSheet = book.createSheet(fileName.substring(fileName.lastIndexOf("_")+ 1, fileName.length()));
            
            
            totalColor = new XSSFColor(new Color(221, 0, 51));
            cellStyle = book.createCellStyle();
            font = book.createFont();
            font.setBold(true);
            font.setFontName("Arial");
            font.setFontHeight(9);
            
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            for(Header header: headers)
            {
                folioNumberSheet.addMergedRegion(new CellRangeAddress(countRow, countRow, 0, 9));
                row = folioNumberSheet.createRow(countRow);
                countRow = countRow + 1; 
                
                checkHeader = false;
                for(int a = 0; a < 3; a++)
                {
                    switch(a)
                    {
                        case 0:
                            if(StringUtils.isNotBlank(header.getColumn1()) && StringUtils.isNotEmpty(header.getColumn1()))
                            {
                                if(header.getColumn1().toUpperCase().contains("MONEDA:") == false)
                                {
                                    cell = row.createCell(a);
                                    cell.setCellValue(header.getColumn1()); 
                                    cell.setCellStyle(cellStyle);
                                    checkHeader = true;
                                }
                            }
                            break;
                        case 1:
                            if(StringUtils.isNotBlank(header.getColumn2()) && StringUtils.isNotEmpty(header.getColumn2()))
                            {
                                if(header.getColumn2().toUpperCase().contains("MONEDA:") == false)
                                {
                                    cell = row.createCell(0);
                                    cell.setCellValue(header.getColumn2());
                                    cell.setCellStyle(cellStyle);
                                    checkHeader = true;
                                }
                            }
                            break;
                        case 2: 
                            if(StringUtils.isNotBlank(header.getColumn3()) && StringUtils.isNotEmpty(header.getColumn3()))
                            {
                                if(header.getColumn3().toUpperCase().contains("MONEDA:") == false)
                                {
                                    cell = row.createCell(0);
                                    cell.setCellValue(header.getColumn3());
                                    cell.setCellStyle(cellStyle);
                                    checkHeader = true;
                                }
                            }
                            break;
                    }
                    
                    if(checkHeader == true)
                    {
                        a = 3; 
                    }
                }
            }
            
            countRow = countRow + 1; 
            row = folioNumberSheet.createRow(countRow);
            
            countRow = countRow + 1; 
            lastFreezeRowIndex = countRow; 
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
            
            cellStyle = book.createCellStyle();
            
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            cellDateStyle = book.createCellStyle();
            createHelper = book.getCreationHelper();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            cellDateStyle.setFont(font);
            cellDateStyle.setAlignment(HorizontalAlignment.CENTER);
            cellDateStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-mmm-yy"));
            
            currencyStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            currencyStyle.setDataFormat((short)8);
            currencyStyle.setFont(font);
            currencyStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            totalColumnStyle = book.createCellStyle();
            font = book.createFont();
            font.setBold(true);
            font.setFontName("Arial");
            font.setColor(totalColor);
            font.setFontHeight(9);
            totalColumnStyle.setDataFormat((short)8);
            totalColumnStyle.setFont(font);
            totalColumnStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            totalStyle = book.createCellStyle();
            totalFont = book.createFont();
            totalFont.setFontName("Arial");
            totalFont.setFontHeight(7.5);
            totalStyle.setFont(totalFont);
            totalStyle.setAlignment(HorizontalAlignment.LEFT);
            
            firstRowIndex = countRow + 1;
            
            for(Perception perception: perceptions)
            {
                validation = null; 
                validation = perception.getValidacion();
                if(validation != null)
                {
                    if(perception.getMtvo().equals("RECEPCIONES"))
                    {
                        row = folioNumberSheet.createRow(countRow);
                        if(validation.isMtvo() && validation.isTienda() && validation.isNumeroRecibo() && validation.isNumeroOrden() && validation.isNumeroPedidoAdicional() && validation.isNumeroRemision() && validation.isFecha() && validation.isValor() && validation.isIva() && validation.isNeto())
                        {
                            for(int b = 0; b < 10; b++)
                            {
                                switch(b)
                                {
                                    case 0: cell = row.createCell(b); cell.setCellValue(perception.getMtvo()); cell.setCellStyle(cellStyle); break;
                                    case 1: cell = row.createCell(b); cell.setCellValue(perception.getTienda()); cell.setCellStyle(cellStyle); break;
                                    case 2: cell = row.createCell(b); cell.setCellValue(perception.getNumeroRecibo()); cell.setCellStyle(cellStyle); break;
                                    case 3: cell = row.createCell(b); cell.setCellValue(perception.getNumeroOrden()); cell.setCellStyle(cellStyle); break;
                                    case 4: cell = row.createCell(b); cell.setCellValue(perception.getNumeroPedidoAdicional()); cell.setCellStyle(cellStyle); break;
                                    case 5: cell = row.createCell(b); cell.setCellValue(perception.getNumeroRemision()); cell.setCellStyle(cellStyle); break;
                                    case 6: cell = row.createCell(b); simpleDateFormat = null; simpleDateFormat = new SimpleDateFormat("dd-MMM-yy", Locale.US); date = null;  date = simpleDateFormat.parse(perception.getFecha()); cell.setCellValue(date); cell.setCellStyle(cellDateStyle); break;
                                    case 7: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = perception.getValor().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                                    case 8: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = perception.getIva().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                                    case 9: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = perception.getNeto().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                                }
                            }
                            countRow = countRow + 1;
                        }
                    }
                }
                
            }
            
            for(Perception cleanPerception: cleanPerceptions)
            {
                validation = null; 
                validation = cleanPerception.getValidacion();
                if(validation != null)
                {
                    if(cleanPerception.getMtvo().equals("RECEPCIONES"))
                    {
                        row = folioNumberSheet.createRow(countRow);
                        if(validation.isMtvo() && validation.isTienda() && validation.isNumeroRecibo() && validation.isNumeroOrden() && validation.isNumeroPedidoAdicional() && validation.isNumeroRemision() && validation.isFecha() && validation.isValor() && validation.isIva() && validation.isNeto())
                        {
                            for(int b = 0; b < 10; b++)
                            {
                                switch(b)
                                {
                                    case 0: cell = row.createCell(b); cell.setCellValue(cleanPerception.getMtvo()); cell.setCellStyle(cellStyle); break;
                                    case 1: cell = row.createCell(b); cell.setCellValue(cleanPerception.getTienda()); cell.setCellStyle(cellStyle); break;
                                    case 2: cell = row.createCell(b); cell.setCellValue(cleanPerception.getNumeroRecibo()); cell.setCellStyle(cellStyle); break;
                                    case 3: cell = row.createCell(b); cell.setCellValue(cleanPerception.getNumeroOrden()); cell.setCellStyle(cellStyle); break;
                                    case 4: cell = row.createCell(b); cell.setCellValue(cleanPerception.getNumeroPedidoAdicional()); cell.setCellStyle(cellStyle); break;
                                    case 5: cell = row.createCell(b); cell.setCellValue(cleanPerception.getNumeroRemision()); cell.setCellStyle(cellStyle); break;
                                    case 6: cell = row.createCell(b); simpleDateFormat = null; simpleDateFormat = new SimpleDateFormat("dd-MMM-yy", Locale.US); date = null;  date = simpleDateFormat.parse(cleanPerception.getFecha()); cell.setCellValue(date); cell.setCellStyle(cellDateStyle); break;
                                    case 7: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = cleanPerception.getValor().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                                    case 8: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = cleanPerception.getIva().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                                    case 9: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = cleanPerception.getNeto().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                                }
                            }
                            countRow = countRow + 1;
                        }
                    }
                }
            }
            
            row = folioNumberSheet.createRow(countRow);
            lastRowIndex = countRow + 1;
            
            folioNumberSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 2, 5), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            folioNumberSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 7, 9), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            
            countRow = countRow + 1; 
            
            row = folioNumberSheet.createRow(countRow);
            
            for(int c = 6; c < 10; c++)
            {
                switch(c)
                {
                    case 6: cell = row.createCell(c); cell.setCellValue("TOTAL:"); cell.setCellStyle(totalColumnStyle); break; 
                    case 7: cell = row.createCell(c); cell.setCellType(CellType.FORMULA); cell.setCellFormula("SUM(H"+ firstRowIndex +":H"+ lastRowIndex +")"); cell.setCellStyle(totalColumnStyle); break;
                    case 8: cell = row.createCell(c); cell.setCellType(CellType.FORMULA); cell.setCellFormula("SUM(I"+ firstRowIndex +":I"+ lastRowIndex +")"); cell.setCellStyle(totalColumnStyle); break;
                    case 9: cell = row.createCell(c); cell.setCellType(CellType.FORMULA); cell.setCellFormula("SUM(J"+ firstRowIndex +":J"+ lastRowIndex +")"); cell.setCellStyle(totalColumnStyle); break;
                }
            }
            
            //Evalua todas las formulas para que funcione el autoSizeColumn
            
            
            folioNumberSheet.setAutoFilter(new CellRangeAddress(firstRowIndex - 2, firstRowIndex - 2, 0, columnNames.size() - 1));
            folioNumberSheet.createFreezePane(0, lastFreezeRowIndex);
            
            XSSFFormulaEvaluator.evaluateAllFormulaCells(book);
            
            for(int d = 0; d < columnNames.size(); d++)
            {
                folioNumberSheet.autoSizeColumn(d);
            }
            
            cell = row.getCell(6); cell.setCellStyle(totalStyle);
            
            
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
    
    public static void createRefundSheet(IJidokaServer<?> server, String fileName, ArrayList<String> columnNames, ArrayList<Perception> refunds, ArrayList<String> idSAPBranch)
    {
        try
        {
            boolean checkHeader = false;
            int countRow = 0; 
            int countIdSapBranch = 0;
            int firstRowIndex = 0;
            int lastRowIndex = 0;
            int lastFreezeRowIndex = 0; 
            double currencyContent = 0.0; 
            Row row = null;
            Cell cell = null;
            XSSFCellStyle cellStyle = null;
            XSSFCellStyle cellDateStyle = null;
            XSSFCellStyle currencyStyle = null;
            XSSFCellStyle sumStyle = null;
            XSSFCellStyle branchStyle = null;
            
            CellStyle totalColumnStyle = null;
            CellStyle totalStyle = null;
            
            XSSFColor columnColor = null;
            XSSFColor totalColor = null;
            XSSFColor backgroundColor = null;
            XSSFFont font = null;
            XSSFFont totalFont = null;
            
            CreationHelper createHelper = null; 
            FileOutputStream outputStream = null;
            Validation validation = null;
            String currencyColumnContent = ""; 
            NumberFormat currencyFormat = null; 
            SimpleDateFormat simpleDateFormat = null; 
            Date date = null; 
            StringBuilder excelFilePath = new StringBuilder(); 
            excelFilePath.append(server.getCurrentDir());
            excelFilePath.append("\\");
            excelFilePath.append(fileName);
            excelFilePath.append(".xlsx");
            
            FileInputStream inputFile =new FileInputStream(new File(excelFilePath.toString()));
            XSSFWorkbook book = new XSSFWorkbook(inputFile);
            XSSFSheet refundSheet = book.createSheet("Devoluciones");
            
            
            totalColor = new XSSFColor(new Color(221, 0, 51));
            cellStyle = book.createCellStyle();
            font = book.createFont();
            font.setBold(true);
            font.setFontName("Arial");
            font.setFontHeight(9);
            
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            
            lastFreezeRowIndex = countRow; 
            columnColor = new XSSFColor(new Color(119, 119, 170)); 
            
            cellStyle = book.createCellStyle();

            font = book.createFont();
            font.setColor(columnColor);
            font.setBold(true);
            font.setFontName("Arial");
            font.setFontHeightInPoints((short) 9);
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            row = refundSheet.createRow(countRow); 
            for(int b = 0; b < columnNames.size(); b++)
            {
                cell = row.createCell(b); 
                cell.setCellValue(columnNames.get(b));
                cell.setCellStyle(cellStyle);
            }
            
            cellStyle = book.createCellStyle();
            backgroundColor = new XSSFColor(new Color(255, 192, 0));
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            cellStyle.setFont(font);
            cellStyle.setFillForegroundColor(backgroundColor);
            cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            branchStyle = book.createCellStyle(); 
            branchStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            cellDateStyle = book.createCellStyle();
            createHelper = book.getCreationHelper();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            cellDateStyle.setFont(font);
            cellDateStyle.setFillForegroundColor(backgroundColor);
            cellDateStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            cellDateStyle.setAlignment(HorizontalAlignment.CENTER);
            cellDateStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-mmm-yy"));
            
            currencyStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            currencyStyle.setDataFormat((short)8);
            currencyStyle.setFont(font);
            currencyStyle.setFillForegroundColor(backgroundColor);
            currencyStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            currencyStyle.setAlignment(HorizontalAlignment.CENTER);
            currencyStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            sumStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            sumStyle.setDataFormat((short)8);
            sumStyle.setFont(font);
            sumStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            totalColumnStyle = book.createCellStyle();
            font = book.createFont();
            font.setBold(true);
            font.setFontName("Arial");
            font.setColor(totalColor);
            font.setFontHeight(9);
            totalColumnStyle.setDataFormat((short)8);
            totalColumnStyle.setFont(font);
            totalColumnStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            totalStyle = book.createCellStyle();
            totalFont = book.createFont();
            totalFont.setFontName("Arial");
            totalFont.setFontHeight(7.5);
            totalStyle.setFont(totalFont);
            totalStyle.setAlignment(HorizontalAlignment.LEFT);
            
            firstRowIndex = countRow + 1;
            
            countRow = countRow + 1; 
            for(Perception perception: refunds)
            {
                row = refundSheet.createRow(countRow);
                for(int b = 0; b < 12; b++)
                {
                    switch(b)
                    {
                        case 0: cell = row.createCell(b); cell.setCellValue(perception.getMtvo()); cell.setCellStyle(cellStyle); break;
                        case 1: cell = row.createCell(b); cell.setCellValue(perception.getTienda()); cell.setCellStyle(cellStyle); break;
                        case 2: cell = row.createCell(b); cell.setCellValue(perception.getNumeroRecibo()); cell.setCellStyle(cellStyle); break;
                        case 3: cell = row.createCell(b); cell.setCellValue(perception.getNumeroOrden()); cell.setCellStyle(cellStyle); break;
                        case 4: cell = row.createCell(b); cell.setCellValue(perception.getNumeroPedidoAdicional()); cell.setCellStyle(cellStyle); break;
                        case 5: cell = row.createCell(b); cell.setCellValue(perception.getNumeroRemision()); cell.setCellStyle(cellStyle); break;
                        case 6: cell = row.createCell(b); simpleDateFormat = null; simpleDateFormat = new SimpleDateFormat("dd-MMM-yy", Locale.US); date = null;  date = simpleDateFormat.parse(perception.getFecha()); cell.setCellValue(date); cell.setCellStyle(cellDateStyle); break;
                        case 7: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = perception.getValor().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                        case 8: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = perception.getIva().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                        case 9: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = perception.getNeto().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                        case 10: cell = row.createCell(b); break;
                        case 11: cell = row.createCell(b); cell.setCellValue(idSAPBranch.get(countIdSapBranch)); cell.setCellStyle(branchStyle); break;
                    }
                }
                countRow = countRow + 1;
                countIdSapBranch = countIdSapBranch + 1; 
            }
            
            row = refundSheet.createRow(countRow);
            lastRowIndex = countRow + 1;
            
            refundSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 2, 5), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            refundSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 7, 11), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            
            countRow = countRow + 1; 
            
            row = refundSheet.createRow(countRow);
            
            for(int c = 6; c < 10; c++)
            {
                switch(c)
                {
                    case 6: cell = row.createCell(c); cell.setCellValue("TOTAL:"); cell.setCellStyle(totalColumnStyle); break; 
                    case 7: cell = row.createCell(c); cell.setCellType(CellType.FORMULA); cell.setCellFormula("SUM(H"+ firstRowIndex +":H"+ lastRowIndex +")"); cell.setCellStyle(totalColumnStyle); break;
                    case 8: cell = row.createCell(c); cell.setCellType(CellType.FORMULA); cell.setCellFormula("SUM(I"+ firstRowIndex +":I"+ lastRowIndex +")"); cell.setCellStyle(totalColumnStyle); break;
                    case 9: cell = row.createCell(c); cell.setCellType(CellType.FORMULA); cell.setCellFormula("SUM(J"+ firstRowIndex +":J"+ lastRowIndex +")"); cell.setCellStyle(totalColumnStyle); break;
                }
            }
            
            
            //Evalua todas las formulas para que funcione el autoSizeColumn

            
            XSSFFormulaEvaluator.evaluateAllFormulaCells(book);
            
            for(int d = 0; d < columnNames.size(); d++)
            {
                refundSheet.autoSizeColumn(d);
            }
            
            
            for(int e = 6; e <= 10; e++)
            {
                switch(e)
                {
                    case 6: cell = row.getCell(e); cell.setCellValue(""); cell.setCellStyle(null); break;
                    case 7: cell = row.getCell(e); row.removeCell(cell); break;
                    case 8: cell = row.getCell(e); row.removeCell(cell); break;
                    case 9: cell = row.getCell(e); cell.setCellStyle(sumStyle); break;
                }
            }
            
            refundSheet.setAutoFilter(new CellRangeAddress(firstRowIndex - 2, firstRowIndex - 2, 0, columnNames.size() - 1));
            
            row = refundSheet.getRow(0);
            cell = row.getCell(10); cell.setCellValue("");
            
            
            
            
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
    
    public static void createSaleSheet(IJidokaServer<?> server, String fileName, ArrayList<Sale> sales)
    {
        try
        {
            int countRow = 0;
            int lastFreezeRowIndex;
            int lastRowIndex = 0;
            int firstRowIndex = 0;
            Row row = null;
            Cell cell = null;
            XSSFCellStyle cellStyle = null;
            XSSFCellStyle cellDateStyle = null;
            XSSFCellStyle cellSaleStyle = null; 
            XSSFCellStyle cellClientStyle = null; 
            XSSFCellStyle cellDecimalStyle = null; 
            XSSFCellStyle cellNumberStyle = null;
            
            XSSFColor whiteColor = null;
            XSSFColor saleBackgroundColor = null;
            XSSFColor clientBackgroundColor = null;
            XSSFFont font = null;
            XSSFDataFormat dataFormat = null;

            FileOutputStream outputStream = null;
            SimpleDateFormat simpleDateFormat = null;
            SimpleDateFormat newSimpleDateFormat = null;
            Date date = null; 
            Date newDate = null;
            String newDateFormat = "";
            String currencyColumnContent = ""; 
            String[] headerColumns = {"Remisiones", "Cartera", "Diferencia"}; 
            String[] columnNames = {"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura / Remisión Sicav", "Importe", "Cliente", "Ref.fact.", "Referencia", "Clv.ref.2", "Clv.ref.3", "Fecha doc.", "Venc.neto", "ImpteML", "Ce.", "Div."};
            NumberFormat currencyFormat = null; 
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            StringBuilder excelFilePath = new StringBuilder(); 
            excelFilePath.append(server.getCurrentDir());
            excelFilePath.append("\\");
            excelFilePath.append(fileName);
            excelFilePath.append(".xlsx");
            
            CreationHelper createHelper = null; 
            FileInputStream inputFile =new FileInputStream(new File(excelFilePath.toString())); 
            XSSFWorkbook book = new XSSFWorkbook(inputFile);
            XSSFSheet saleSheet = book.createSheet("Venta");
            
            whiteColor = new XSSFColor(Color.WHITE); 
            
            saleBackgroundColor = new XSSFColor(new Color(102, 0, 51)); 
            clientBackgroundColor = new XSSFColor(new Color(32, 55, 100)); 
            cellStyle = book.createCellStyle();

            font = book.createFont();
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.LEFT);
            
            cellSaleStyle = book.createCellStyle();

            font = book.createFont();
            font.setFontName("Calibri");
            font.setColor(whiteColor);
            font.setFontHeight(11);
            cellSaleStyle.setFont(font);
            cellSaleStyle.setFillForegroundColor(saleBackgroundColor);
            cellSaleStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            cellSaleStyle.setAlignment(HorizontalAlignment.LEFT);
            
            cellClientStyle = book.createCellStyle();
            cellClientStyle.setFont(font);
            cellClientStyle.setFillForegroundColor(clientBackgroundColor);
            cellClientStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            cellClientStyle.setAlignment(HorizontalAlignment.LEFT);
            
            row = saleSheet.createRow(countRow);
            
            for(int a = 0; a < headerColumns.length; a++)
            {
                cell = row.createCell(8);
                cell.setCellValue(headerColumns[a]);
                cell.setCellStyle(cellStyle);
                countRow = countRow + 1;
                row = saleSheet.createRow(countRow);
            }
            
            for(int b = 0; b < columnNames.length; b++)
            {
                cell = row.createCell(b); 
                cell.setCellValue(columnNames[b]);
                if(b <= 9)
                {
                    cell.setCellStyle(cellSaleStyle);
                }
                else 
                {
                    cell.setCellStyle(cellClientStyle);
                }
            }
            
            lastFreezeRowIndex = countRow + 1; 
            saleSheet.createFreezePane(0, lastFreezeRowIndex);
            
            cellDateStyle = book.createCellStyle();
            createHelper = book.getCreationHelper();
            font = book.createFont();
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellDateStyle.setFont(font);
            cellDateStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd/mm/yyyy"));
            cellDateStyle.setAlignment(HorizontalAlignment.CENTER);
            
            cellNumberStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellNumberStyle.setFont(font);
            cellNumberStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            cellDecimalStyle = book.createCellStyle();
            dataFormat = book.createDataFormat();
            font = book.createFont();
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellDecimalStyle.setFont(font);
            cellDecimalStyle.setDataFormat(dataFormat.getFormat("#,##0.00"));
            cellDecimalStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            
            cellStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellStyle.setFont(font);
            //cellStyle.setAlignment(HorizontalAlignment.CENTER);
            firstRowIndex = countRow + 1;
            
            for(Sale sale: sales)
            {
                countRow = countRow + 1; 
                row = saleSheet.createRow(countRow);
                for(int b = 0; b < 10; b++)
                {
                    switch(b)
                    {
                        case 0: cell = row.createCell(b); simpleDateFormat = null; simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US); date = null;  date = simpleDateFormat.parse(sale.getFecha()); newSimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy", Locale.US); newDateFormat = newSimpleDateFormat.format(date); newDate = newSimpleDateFormat.parse(newDateFormat); cell.setCellValue(newDate); cell.setCellStyle(cellDateStyle); break;
                        case 1: cell = row.createCell(b); cell.setCellValue(sale.getPedidoAdicional()); cell.setCellStyle(cellNumberStyle); break;
                        case 2: cell = row.createCell(b); cell.setCellValue(sale.getFactura()); cell.setCellStyle(cellNumberStyle); break;
                        case 3: cell = row.createCell(b); cell.setCellValue(sale.getFolio()); cell.setCellStyle(cellStyle); break;
                        case 4: cell = row.createCell(b); cell.setCellValue(sale.getSolicitante()); cell.setCellStyle(cellNumberStyle); break;
                        case 5: cell = row.createCell(b); cell.setCellValue(sale.getCedis()); cell.setCellStyle(cellNumberStyle); break;
                        case 6: cell = row.createCell(b); cell.setCellValue(sale.getDestinatario()); cell.setCellStyle(cellNumberStyle); break;
                        case 7: cell = row.createCell(b); cell.setCellValue(sale.getNombreDestinatario()); cell.setCellStyle(cellStyle); break; 
                        case 8: cell = row.createCell(b); cell.setCellValue(sale.getFacturaRemisionSicav()); cell.setCellStyle(cellStyle); break; 
                        case 9: cell = row.createCell(b); NumberFormat format = NumberFormat.getInstance(Locale.US); Number number = format.parse(sale.getImporte()); cell.setCellValue(number.doubleValue()); cell.setCellStyle(cellDecimalStyle); break;
                    }
                }
            }
            
            lastRowIndex = countRow + 1; 
            saleSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 1, 2), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            saleSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 4, 6), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            saleSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 9, 9), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            
            saleSheet.setZoom(75);
            
            
            XSSFFormulaEvaluator.evaluateAllFormulaCells(book);
            
            for(int d = 0; d < columnNames.length; d++)
            {
                saleSheet.autoSizeColumn(d);
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
    
    public static void createMoorageSheet(IJidokaServer<?> server, String fileName, ArrayList<Link> links)
    {
        try
        {
            int countRow = 0;
            int lastFreezeRowIndex;
            Row row = null;
            Cell cell = null;
            XSSFCellStyle cellStyle = null;
            XSSFCellStyle cellSaleStyle = null; 
            XSSFCellStyle cellStoreStyle = null; 
            XSSFCellStyle cellPercentageStyle = null;
            
            XSSFColor whiteColor = null;
            XSSFColor saleBackgroundColor = null;
            XSSFColor storeBackgroundColor = null;
            XSSFColor percentageBackgroundColor = null;
            
            XSSFFont font = null;
            

            FileOutputStream outputStream = null;
            SimpleDateFormat simpleDateFormat = null;
            Date date = null; 
            String currencyColumnContent = ""; 
            String[] columnNames = {"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", "Importe", "Pedido Adicional", "CR Tienda", "Num de Remisión", "Fecha", "Neto", "Neto", "Diferencia", "%", "Diferencia", "Destinatario", "Tipo de Busqueda"};
            NumberFormat currencyFormat = null; 
            StringBuilder excelFilePath = new StringBuilder(); 
            excelFilePath.append(server.getCurrentDir());
            excelFilePath.append("\\");
            excelFilePath.append(fileName);
            excelFilePath.append(".xlsx");
            
            CreationHelper createHelper = null; 
            FileInputStream inputFile =new FileInputStream(new File(excelFilePath.toString())); 
            XSSFWorkbook book = new XSSFWorkbook(inputFile);
            XSSFSheet moorageSheet = book.createSheet("Amarre");
            
            whiteColor = new XSSFColor(Color.WHITE); 
            saleBackgroundColor = new XSSFColor(new Color(102, 0, 51)); 
            storeBackgroundColor = new XSSFColor(new Color(146, 208, 80)); 
            percentageBackgroundColor = new XSSFColor(new Color(32, 55, 100)); 
            
            cellStyle = book.createCellStyle();

            font = book.createFont();
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.LEFT);
            
            cellSaleStyle = book.createCellStyle();

            font = book.createFont();
            font.setFontName("Calibri");
            font.setColor(whiteColor);
            font.setFontHeight(11);
            cellSaleStyle.setFont(font);
            cellSaleStyle.setFillForegroundColor(saleBackgroundColor);
            cellSaleStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            cellSaleStyle.setAlignment(HorizontalAlignment.CENTER);
            
            cellStoreStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellStoreStyle.setFont(font);
            cellStoreStyle.setFillForegroundColor(storeBackgroundColor);
            cellStoreStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            cellStoreStyle.setAlignment(HorizontalAlignment.CENTER);
            
            cellPercentageStyle = book.createCellStyle();
            font = book.createFont();
            font.setColor(whiteColor);
            font.setFontName("Calibri");
            font.setFontHeight(11);
            cellPercentageStyle.setFont(font);
            cellPercentageStyle.setFillForegroundColor(percentageBackgroundColor);
            cellPercentageStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            cellPercentageStyle.setAlignment(HorizontalAlignment.CENTER);
            
            row = moorageSheet.createRow(countRow);
            
            for(int a = 0; a < columnNames.length; a++)
            {
                cell = row.createCell(a); 
                cell.setCellValue(columnNames[a]);
                if(a <= 10 || a == 18 || a == 21)
                {
                    cell.setCellStyle(cellSaleStyle);
                }
                else if(a > 11 && a < 17)
                {
                    cell.setCellStyle(cellStoreStyle);
                }
                else if(a == 19)
                {
                    cell.setCellStyle(cellPercentageStyle);
                }
            }
            
            lastFreezeRowIndex = countRow + 1; 
            moorageSheet.createFreezePane(0, lastFreezeRowIndex);
            moorageSheet.setZoom(75);
            /*firstRowIndex = countRow + 1; 
            
            cellStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            cellStyle.setFont(font);
            cellStyle.setAlignment(HorizontalAlignment.CENTER);
            
            cellDateStyle = book.createCellStyle();
            createHelper = book.getCreationHelper();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            cellDateStyle.setFont(font);
            cellDateStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-mmm-yy"));
            cellDateStyle.setAlignment(HorizontalAlignment.CENTER);
            
            currencyStyle = book.createCellStyle();
            font = book.createFont();
            font.setFontName("Arial");
            font.setFontHeight(7.5);
            currencyStyle.setFont(font);
            currencyStyle.setDataFormat((short)8);
            currencyStyle.setAlignment(HorizontalAlignment.RIGHT);
            
            
            for(Perception refund: refunds)
            {
                countRow = countRow + 1; 
                row = refundSheet.createRow(countRow);
                for(int b = 0; b < 12; b++)
                {
                    switch(b)
                    {
                        case 0: cell = row.createCell(b); cell.setCellValue(refund.getMtvo()); cell.setCellStyle(cellStyle); break;
                        case 1: cell = row.createCell(b); cell.setCellValue(refund.getTienda()); cell.setCellStyle(cellStyle); break;
                        case 2: cell = row.createCell(b); cell.setCellValue(refund.getNumeroRecibo()); cell.setCellStyle(cellStyle); break;
                        case 3: cell = row.createCell(b); cell.setCellValue(refund.getNumeroOrden()); cell.setCellStyle(cellStyle); break;
                        case 4: cell = row.createCell(b); cell.setCellValue(refund.getNumeroPedidoAdicional()); cell.setCellStyle(cellStyle); break;
                        case 5: cell = row.createCell(b); cell.setCellValue(refund.getNumeroRemision()); cell.setCellStyle(cellStyle); break;
                        case 6: cell = row.createCell(b); simpleDateFormat = null; simpleDateFormat = new SimpleDateFormat("dd-MMM-yy", Locale.US); date = null;  date = simpleDateFormat.parse(refund.getFecha()); cell.setCellValue(date); cell.setCellStyle(cellDateStyle); break;
                        case 7: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = refund.getValor().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break; 
                        case 8: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = refund.getIva().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break; 
                        case 9: cell = row.createCell(b); currencyFormat = NumberFormat.getCurrencyInstance(Locale.US); currencyColumnContent = ""; currencyColumnContent = refund.getNeto().replace("$", ""); currencyColumnContent =  currencyColumnContent.replace(",", ""); currencyContent = 0; currencyContent = Double.parseDouble(currencyColumnContent); cell.setCellValue(currencyContent); cell.setCellStyle(currencyStyle); break;
                    }
                }
            }
            
            lastRowIndex = countRow + 1; 
            refundSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 2, 5), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            refundSheet.addIgnoredErrors(new CellRangeAddress(firstRowIndex - 1, lastRowIndex, 7, 9), IgnoredErrorType.NUMBER_STORED_AS_TEXT);
            
            row = refundSheet.createRow(countRow); 
            
            countRow = countRow + 1; 
            
            row = refundSheet.createRow(countRow);
            
            cell = row.createCell(9); cell.setCellType(CellType.FORMULA); cell.setCellFormula("SUM(I"+ firstRowIndex +":I"+ lastRowIndex +")"); cell.setCellStyle(currencyStyle);
            
            XSSFFormulaEvaluator.evaluateAllFormulaCells(book);
            
            for(int d = 0; d < columnNames.size(); d++)
            {
                refundSheet.autoSizeColumn(d);
            }*/
            
            XSSFFormulaEvaluator.evaluateAllFormulaCells(book);
            
            for(int d = 0; d < columnNames.length; d++)
            {
                moorageSheet.autoSizeColumn(d);
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
    
    /*public static void autoSizeColumns(Workbook workbook) {
        int numberOfSheets = workbook.getNumberOfSheets();
        for (int i = 0; i < numberOfSheets; i++) {
            Sheet sheet = workbook.getSheetAt(i);
            if (sheet.getPhysicalNumberOfRows() > 0) {
                Row row = sheet.getRow(0);
                Iterator<Cell> cellIterator = row.cellIterator();
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    int columnIndex = cell.getColumnIndex();
                    sheet.autoSizeColumn(columnIndex);
                }
            }
        }
    }*/
    
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
