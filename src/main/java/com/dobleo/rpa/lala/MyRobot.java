package com.dobleo.rpa.lala;

import com.dobleo.rpa.database.DatabaseUtilities;
import com.dobleo.rpa.models.Document;
import com.dobleo.rpa.models.Link;
import com.dobleo.rpa.models.Reception;
import com.dobleo.rpa.models.Sale;
import com.novayre.jidoka.client.api.IJidokaRobot;
import com.novayre.jidoka.client.api.IJidokaServer;
import com.novayre.jidoka.client.api.IRobot;
import com.novayre.jidoka.client.api.JidokaFactory;
import com.novayre.jidoka.client.api.annotations.Robot;
import com.novayre.jidoka.windows.api.EShowWindowState;
import com.novayre.jidoka.windows.api.IWindows;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import com.monitorjbl.xlsx.StreamingReader;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * My robot
 * @author jidoka
 *
 */
@Robot
public class MyRobot implements IRobot {

	/**
	 * Pause between actions like persons do
	 */
	private static final int PAUSE = 500;
	
	/**
	 * Server
	 */
	private IJidokaServer<?> server;
	
	/**
	 * Windows module
	 */
	private IWindows windows;
	
	/**
	 * Current item index, base-1
	 */
	private int currentItem = 1;
        private int currentLinkIndex = 0;
        private DatabaseUtilities databaseUtilities;
	private String databasePath;
        private String excelFilePath;
        private String folioNumber;
        private String fileName;
        private static final String PARAM_INPUT_FILE = "inputFile";
        private static final String DATABASE_NAME = "grupolala";
        private static final String TABLE_DOCUMENTS = "documento";
        private static final String TABLE_RECEPTIONS = "recepciones";
        private static final String TABLE_SALES = "ventas";
        private Map<Integer, String> documentColumns;
        private Map<Integer, String> receptionsColumns;
        private Map<Integer, String> salesColumns;
        private ArrayList<Link> listTenPoint; 
	/**
	 * Action "start"
	 * @return
	 * @throws Exception
	 */
	public void start() throws Exception {
		
		server = (IJidokaServer<?>) JidokaFactory.getServer();
		
		windows = IJidokaRobot.getInstance(this);
		
		// we set standard pause after writing or managing mouse
		windows.typingPause(PAUSE);
		windows.mousePause(PAUSE);
		
		// log parameters example
		server.getParameters().entrySet().forEach((e) -> {
			server.debug(String.format("Parámetro [%s] = [%s]", e.getKey(), e.getValue()));
		});

		// other log types availables
		//server.warn("Warn example");
		//server.error("And error example");

		// we set number of items
		// on an actual robot, this number can be got from an application,
		// an Excel datasheet, etc.
		server.setNumberOfItems(1);
                
                databaseUtilities = new DatabaseUtilities();
                documentColumns = null;
                receptionsColumns = null;
                salesColumns = null;
                databasePath = "";
                fileName = server.getParameters().get(PARAM_INPUT_FILE);
                excelFilePath = Paths.get(server.getCurrentDir(), fileName).toString();
	}
        
        public void createDatabase() throws Exception
        {
            server.info("Delete Database if Exist");
            Files.deleteIfExists(Paths.get(server.getCurrentDir(), DATABASE_NAME + ".db"));
            server.info("Try to create Database");
            databasePath = databaseUtilities.createNewDatabase(server, DATABASE_NAME);
            server.info("Initialization documentos Table Columns"); 
            databaseUtilities.documentTableColumns();
            documentColumns = databaseUtilities.getDocumentColumns();
            server.info("Initialization recepciones Table Columns");
            databaseUtilities.receptionTableColumns();
            receptionsColumns = databaseUtilities.getReceptionsColumns();
            server.info("Initialization ventas Table Columns");
            databaseUtilities.saleTableColumns();
            salesColumns = databaseUtilities.getSalesColumns();
            databaseUtilities.createTable(server, databasePath, TABLE_DOCUMENTS, documentColumns);
            databaseUtilities.createTable(server, databasePath, TABLE_RECEPTIONS, receptionsColumns);
            databaseUtilities.createTable(server, databasePath, TABLE_SALES, salesColumns);
            
        }
        
        public void readExcelFileInformation() throws Exception
        {
            int idFolio = 0;
            int totalCellBlank = 0; 
            int totalColumn = 0;
            short columnNumber = 0;
            short maxColumnNumber = 0;
            boolean columnNameSale = false;
            String cellValue;
            Reception reception = null;
            Sale sale = new Sale();
            Document document = new Document();
            Row row = null;
            Cell cell = null;
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            Iterator<Cell> iteratorCell;
            ArrayList<Cell> listCell = new ArrayList();
            ArrayList<Reception> listReception = new ArrayList<Reception>();
            ArrayList<Sale> listSale = new ArrayList<Sale>(); 
            server.info("Get input File Information");
            String fileName = server.getParameters().get(PARAM_INPUT_FILE);
            server.info("Obtain Excel File Path");
            excelFilePath = Paths.get(server.getCurrentDir(), fileName).toString();
            server.info("Excel File Path: " + excelFilePath);
            FileInputStream in =new FileInputStream(new File(excelFilePath));
            //Workbook book = WorkbookFactory.create(in);
            Workbook book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(in);
            String folioNumber = book.getSheetName(1);
            Sheet sheet = book.getSheetAt(1);
            Iterator<Row> iteratorRow = sheet.iterator();
            
            document.setNumeroFolio(folioNumber);
            document.setNombre(fileName);
            server.info("Insert into documento Table Information");
            databaseUtilities.insertIntoDocument(server, databasePath, TABLE_DOCUMENTS, documentColumns, document);
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            long cantidadColumnas = 0;
            String informacionFila = null; 
            int columnSubNumber = 0;

            DataFormatter formatter = new DataFormatter(); //creating formatter using the default locale
            while(iteratorRow.hasNext())
            {
                row = null;
                row = iteratorRow.next();
                columnNumber = row.getLastCellNum();
                
                if(columnNumber > maxColumnNumber)
                {
                    maxColumnNumber = columnNumber;
                    //System.out.println("Maximo de columnas: " + maxColumnNumber);
                }

                if(maxColumnNumber > 10)
                {
                    totalColumn = maxColumnNumber - 1; 
                    //row = iteratorRow.next();
                    iteratorCell = row.iterator(); 
                    columnSubNumber = 0; 
                    informacionFila = "";
                    reception = null;
                    reception = new Reception();
                    reception.setIdFolio(idFolio);
                    totalCellBlank = 0; 
                    while(iteratorCell.hasNext())
                    {
                        formatter = new DataFormatter();
                        cellValue = formatter.formatCellValue(iteratorCell.next()); 
                        if(StringUtils.isBlank(cellValue))
                        {
                            totalCellBlank = totalCellBlank + 1; 
                        }
                    }
                    
                    if(totalColumn == 10 && totalCellBlank < 6)
                    {
                        for(int a = 0; a < totalColumn; a++)
                        {
                            formatter = new DataFormatter();
                            cellValue = formatter.formatCellValue(row.getCell(a));
                            switch(a)
                            {
                                case 0: reception.setMtvo(cellValue); break;
                                case 1: reception.setTienda(cellValue); break;
                                case 2: reception.setRecibo(cellValue); break;
                                case 3: reception.setOrden(cellValue); break;
                                case 4: reception.setAdicional(cellValue); break;
                                case 5: reception.setRemision(cellValue); break;
                                case 6: SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
                                Date date = row.getCell(a).getDateCellValue(); //dateFormat.parse(cellValue);
                                dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                reception.setFecha(dateFormat.format(date));
                                break;
                                case 7: reception.setValor(decimalFormat.format(row.getCell(a).getNumericCellValue())); break;
                                case 8: reception.setIva(cellValue.replace("$", "")); break; 
                                case 9: reception.setNeto(decimalFormat.format(row.getCell(a).getNumericCellValue())); break;
                                default: break;
                            }

                        }
                        databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                    }
                }

            }
            server.info("Read Sheet " + folioNumber + "Excel File Successfully"); 
            
            server.info("Add Reception Information into Database"); 
            
            sheet = null; 
            iteratorRow = null;
            columnNumber = 0; 
            maxColumnNumber = 0; 
            sheet = book.getSheet("Venta");
            iteratorRow = sheet.iterator();
            while(iteratorRow.hasNext())
            {
                row = null;
                row = iteratorRow.next();
                Date dateTest = row.getCell(0).getDateCellValue(); //dateF
                if(dateTest == null)
                {
                    break;
                }
                columnNumber = row.getLastCellNum();
                
                if(columnNumber > maxColumnNumber)
                {
                    maxColumnNumber = columnNumber;
                    //System.out.println("Maximo de columnas: " + maxColumnNumber);
                }

                if(maxColumnNumber == 20)
                {
                    columnSubNumber = 0; 
                    sale = null;
                    sale = new Sale();
                    sale.setIdFolio(idFolio);
                   
                    
                    if(maxColumnNumber == 20 && columnNameSale == true)
                    {
                        for(int c = 0; c < maxColumnNumber; c++)
                        {
                            formatter = new DataFormatter();
                            cellValue = formatter.formatCellValue(row.getCell(c));
                            switch(c)
                            {
                                case 0: SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                Date date = row.getCell(c).getDateCellValue(); //dateF
                                sale.setFecha(dateFormat.format(date));
                                break;
                                case 1: sale.setPedidoAdicional(cellValue); break;
                                case 2: sale.setFactura(cellValue); break;
                                case 3: sale.setFolio(cellValue); break;
                                case 4: sale.setSolicitante(cellValue); break;
                                case 5: sale.setCedis(cellValue); break;
                                case 6: sale.setDestinatario(cellValue); break;
                                case 7: sale.setNombreDestinatario(cellValue); break;
                                case 8: sale.setFacturaRemisionSicav(cellValue); break; 
                                case 9: if(StringUtils.isBlank(cellValue)){ sale.setImporte(null); } else { sale.setImporte(decimalFormat.format(row.getCell(c).getNumericCellValue())); } break;
                                case 10: sale.setCliente(cellValue); break;
                                case 11: sale.setRefFact(cellValue); break;
                                case 12: sale.setReferencia(cellValue);  break;
                                case 13: sale.setClvRef2(cellValue); break;
                                case 14: sale.setClvRef3(cellValue); break;
                                case 15: if(StringUtils.isBlank(cellValue)){ sale.setFechaDoc(null);} else {SimpleDateFormat dateFormat2 = new SimpleDateFormat("dd/MM/yyyy");
                                Date date2 = row.getCell(c).getDateCellValue();
                                sale.setFechaDoc(dateFormat2.format(date2));
                                }
                                break;
                                case 16: sale.setVencNeto(cellValue); break;
                                case 17: if(StringUtils.isBlank(cellValue)) { sale.setImpteMl(null); } else { sale.setImpteMl(decimalFormat.format(row.getCell(c).getNumericCellValue())); } break;
                                case 18: sale.setCe(cellValue); break;
                                case 19: sale.setDiv(cellValue); break;
                                default: break;
                            }

                        }
                        
                        databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, sale); 
                    }
                    else if(maxColumnNumber == 20 && columnNameSale == false)
                    {
                        columnNameSale = true; 
                    }
                }

            }
            
            server.info("Read Sheet Venta Excel File Successfully"); 
            
            server.info("Add Sale Information into Database"); 
            
            server.info("Read Complete Excel File was Successfull");
            
            //System.out.println("Creado para ambos formatos");
        }
        
        public void readExcelFolioSheet() throws Exception
        {
            folioNumber = null;
            fileName = null;
            Document document = new Document(); 
            Workbook book = null;
            server.info("Open File Excel"); 
            InputStream inputFile = new FileInputStream(new File(excelFilePath));
            server.info("Get input File Information");
            fileName = server.getParameters().get(PARAM_INPUT_FILE);
            server.info("Obtain Excel File Path");
            excelFilePath = Paths.get(server.getCurrentDir(), fileName).toString();
            server.info("Excel File Path: " + excelFilePath);
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Name from Second Sheet into Excel");
            folioNumber = book.getSheetName(1);
            server.info("Name of Second Sheet is: " + folioNumber);
            server.info("Set information to Document object");
            document.setNumeroFolio(folioNumber);
            document.setNombre(fileName);
            document.setPorcentajeIncidencia("0.05");
            server.info("Insert into documento Table Information");
            databaseUtilities.insertIntoDocument(server, databasePath, TABLE_DOCUMENTS, documentColumns, document);
            server.info("Insert into documento Table was successful");
            server.info("Close Excel File"); 
            book.close();
        }
        
        public void readExcelReceptionSheet() throws Exception
        {
            int idFolio = 0;
            int columnNumberRow = 0;
            int maxColumnNumberRow = 0;
            int totalCellBlank = 0;
            int totalColumn = 0;
            int columnIndex = 0;
            String cellValue = null;
            Document document = new Document();
            Reception reception = new Reception();
            Workbook book = null;
            //Row row = null;
            Sheet receptionSheet = null;
            InputStream inputFile = null;
            Iterator<Cell> iteratorCell = null;
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            Date date = null;
            SimpleDateFormat dateFormat = null;
            DataFormatter dataFormatter = new DataFormatter();
            server.info("Set Information into document Object");
            document.setNumeroFolio(folioNumber);
            document.setNombre(fileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(excelFilePath));
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Sheet from Excel File");
            receptionSheet = book.getSheetAt(1);
            server.info("Read All Filled Row From Sheet: " + receptionSheet.getSheetName()); 
            server.info("Insert Data Information Into recepciones Table");
            /*for(int a = 0; a < receptionSheet.getLastRowNum(); a++)
            {
                row = receptionSheet.getRow(a);
                if(row == null)
                {
                    break;
                }
                
                columnNumberRow = row.getLastCellNum();
                if(columnNumberRow > maxColumnNumberRow)
                {
                    maxColumnNumberRow = columnNumberRow;
                }
                
                if(maxColumnNumberRow > 10)
                {
                    totalCellBlank = 0;
                    iteratorCell = null;
                    reception = null;
                    reception = new Reception();
                    reception.setIdFolio(idFolio);
                    totalColumn = maxColumnNumberRow - 1;
                    iteratorCell = row.cellIterator();
                    
                    while(iteratorCell.hasNext())
                    {
                        dataFormatter = new DataFormatter();
                        cellValue = dataFormatter.formatCellValue(iteratorCell.next()); 
                        if(StringUtils.isBlank(cellValue))
                        {
                            totalCellBlank = totalCellBlank + 1; 
                        }
                    }
                    
                    if(totalColumn == 10 && totalCellBlank < 6)
                    {
                        for(int b = 0; b < totalColumn; b++)
                        {
                            dataFormatter = new DataFormatter();
                            cellValue = null;
                            cellValue = dataFormatter.formatCellValue(row.getCell(b));
                            switch(b)
                            {
                                case 0: reception.setMtvo(cellValue); break;
                                case 1: reception.setTienda(cellValue); break;
                                case 2: reception.setRecibo(cellValue); break;
                                case 3: reception.setOrden(cellValue); break;
                                case 4: reception.setAdicional(cellValue); break;
                                case 5: reception.setRemision(cellValue); break;
                                case 6: date = row.getCell(b).getDateCellValue(); 
                                dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                reception.setFecha(dateFormat.format(date));
                                break;
                                case 7: reception.setValor(decimalFormat.format(row.getCell(b).getNumericCellValue())); break;
                                case 8: reception.setIva(cellValue.replace("$", "")); break; 
                                case 9: reception.setNeto(decimalFormat.format(row.getCell(b).getNumericCellValue())); break;
                                default: break;
                            }

                        }
                        databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                    }
                    
                    
                }
                
            }*/
            for(Row row: receptionSheet)
            {
                if(row == null)
                {
                    break;
                }
                
                columnNumberRow = row.getLastCellNum();
                if(columnNumberRow > maxColumnNumberRow)
                {
                    maxColumnNumberRow = columnNumberRow;
                }
                
                if(maxColumnNumberRow > 10)
                {
                    totalCellBlank = 0;
                    iteratorCell = null;
                    reception = null;
                    reception = new Reception();
                    reception.setIdFolio(idFolio);
                    totalColumn = maxColumnNumberRow - 1;
                    iteratorCell = row.cellIterator();
                    
                    for (Cell cellTemp : row) 
                    {
                        dataFormatter = new DataFormatter();
                        cellValue = dataFormatter.formatCellValue(cellTemp); 
                        if(StringUtils.isBlank(cellValue))
                        {
                            totalCellBlank = totalCellBlank + 1; 
                        }
                    }
                    
                    if(totalColumn == 10 && totalCellBlank < 6)
                    {
                        columnIndex = 0; 
                        for(int b = 0; b < totalColumn; b++)
                        {
                            dataFormatter = new DataFormatter();
                            cellValue = null;
                            cellValue = dataFormatter.formatCellValue(row.getCell(b));
                            //cellValue = cell.getStringCellValue();
                            //if(columnIndex <= 9)
                            //{
                                switch(b)
                                {
                                    case 0: if(StringUtils.isBlank(cellValue)){reception.setMtvo("");}else{reception.setMtvo(cellValue);} columnIndex++; break;
                                    case 1: if(StringUtils.isBlank(cellValue)){reception.setTienda("");}else{reception.setTienda(cellValue);} columnIndex++; break;
                                    case 2: if(StringUtils.isBlank(cellValue)){reception.setRecibo("");}else{reception.setRecibo(cellValue);} columnIndex++; break;
                                    case 3: if(StringUtils.isBlank(cellValue)){reception.setOrden("");}else{reception.setOrden(cellValue);} columnIndex++; break;
                                    case 4: if(StringUtils.isBlank(cellValue)){reception.setAdicional("");}else{reception.setAdicional(cellValue);} columnIndex++; break;
                                    case 5: if(StringUtils.isBlank(cellValue)){reception.setRemision("");}else{reception.setRemision(cellValue);} columnIndex++; break;
                                    case 6: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        reception.setFecha("");
                                    }
                                    else
                                    {
                                        date = row.getCell(b).getDateCellValue(); 
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        reception.setFecha(dateFormat.format(date));
                                    }
                                    columnIndex++;
                                    break;
                                    case 7: if(StringUtils.isBlank(cellValue)){reception.setValor("");}else{reception.setValor(decimalFormat.format(row.getCell(b).getNumericCellValue()));} columnIndex++; break;
                                    case 8: if(StringUtils.isBlank(cellValue)){reception.setIva("");}else{reception.setIva(cellValue.replace("$", ""));} columnIndex++; break; 
                                    case 9: if(StringUtils.isBlank(cellValue)){reception.setNeto("");}else{reception.setNeto(decimalFormat.format(row.getCell(b).getNumericCellValue()));} columnIndex++; break;
                                    default: break;
                                }
                            //}
                            
                        }
                        databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                    }
                    
                    
                }
            }
            server.info("Insert Data Information into recepciones Table was sucessful");
            server.info("Close Excel File"); 
            book.close();
            
        }
        
        public void readExcelSaleSheet() throws Exception
        {
            boolean checkSaleColumns = false;
            int idFolio = 0;
            short columnNumberRow = 0;
            short maxColumnNumberRow = 0; 
            String cellValue = null;
            Document document = new Document();
            Sale sale = new Sale();
            Workbook book = null;
            Sheet saleSheet = null;
            Row row = null; 
            InputStream inputFile = null;
            SimpleDateFormat dateFormat = null;
            Date date = null;
            DataFormatter dataFormatter = new DataFormatter();
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            server.info("Set Information into document Object");
            document.setNumeroFolio(folioNumber);
            document.setNombre(fileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(excelFilePath));
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().open(inputFile);
            server.info("Get Sheet from Excel File");
            saleSheet = book.getSheetAt(3);
            server.info("Read All Filled Row From Sheet: " + saleSheet.getSheetName()); 
            server.info("Insert Data Information Into ventas Table");
            for(int a = 0; a < saleSheet.getLastRowNum(); a++)
            {
                row = null;
                row = saleSheet.getRow(a); 
                if(row == null)
                {
                    break;
                }
                
                columnNumberRow = row.getLastCellNum();
                if(columnNumberRow > maxColumnNumberRow)
                {
                    maxColumnNumberRow = columnNumberRow;
                }
                
                if(maxColumnNumberRow == 20)
                {
                    sale = null; 
                    sale = new Sale(); 
                    sale.setIdFolio(idFolio);
                    
                    if(maxColumnNumberRow == 20 && checkSaleColumns == true)
                    {
                        if(row != null)
                        {
                            if(row.getCell(0) != null)
                            {
                                date = row.getCell(0).getDateCellValue();
                                if(date == null)
                                {
                                    break;
                                }
                            }
                        }
                        
                        for(int b = 0; b < maxColumnNumberRow; b++)
                        {
                            dataFormatter = new DataFormatter();
                            cellValue = dataFormatter.formatCellValue(row.getCell(b));
                            switch(b)
                            {
                                case 0: dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                date = row.getCell(b).getDateCellValue(); 
                                sale.setFecha(dateFormat.format(date));
                                break;
                                case 1: sale.setPedidoAdicional(cellValue); break;
                                case 2: sale.setFactura(cellValue); break;
                                case 3: sale.setFolio(cellValue); break;
                                case 4: sale.setSolicitante(cellValue); break;
                                case 5: sale.setCedis(cellValue); break;
                                case 6: sale.setDestinatario(cellValue); break;
                                case 7: sale.setNombreDestinatario(cellValue); break;
                                case 8: sale.setFacturaRemisionSicav(cellValue); break; 
                                case 9: if(StringUtils.isBlank(cellValue)){ sale.setImporte(null); } else { sale.setImporte(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 10: sale.setCliente(cellValue); break;
                                case 11: sale.setRefFact(cellValue); break;
                                case 12: sale.setReferencia(cellValue);  break;
                                case 13: sale.setClvRef2(cellValue); break;
                                case 14: sale.setClvRef3(cellValue); break;
                                case 15: if(StringUtils.isBlank(cellValue)){ sale.setFechaDoc(null);} else { dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                date = row.getCell(b).getDateCellValue();
                                sale.setFechaDoc(dateFormat.format(date));
                                }
                                break;
                                case 16: sale.setVencNeto(cellValue); break;
                                case 17: if(StringUtils.isBlank(cellValue)) { sale.setImpteMl(null); } else { sale.setImpteMl(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 18: sale.setCe(cellValue); break;
                                case 19: sale.setDiv(cellValue); break;
                                default: break;
                            }
                        }
                        databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, sale); 
                    }
                    else if(maxColumnNumberRow == 20 && checkSaleColumns == false)
                    {
                        checkSaleColumns = true;
                    }
                }
            }
            /*for(Row row: saleSheet)
            {
                if(row == null)
                {
                    break;
                }
                
                columnNumberRow = row.getLastCellNum();
                if(columnNumberRow > maxColumnNumberRow)
                {
                    maxColumnNumberRow = columnNumberRow;
                }
                
                if(maxColumnNumberRow == 20)
                {
                    sale = null; 
                    sale = new Sale(); 
                    sale.setIdFolio(idFolio);
                    
                    if(maxColumnNumberRow == 20 && checkSaleColumns == true)
                    {
                        if(row != null)
                        {
                            if(row.getCell(0) != null)
                            {
                                date = row.getCell(0).getDateCellValue();
                                if(date == null)
                                {
                                    break;
                                }
                            }
                        }
                        
                        for(int b = 0; b < maxColumnNumberRow; b++)
                        {
                            dataFormatter = new DataFormatter();
                            cellValue = dataFormatter.formatCellValue(row.getCell(b));
                            server.info(cellValue); 
                            //cellValue = row.getCell(b).getStringCellValue();
                            switch(b)
                            {
                                case 0: 
                                if(StringUtils.isBlank(cellValue))
                                {
                                    sale.setFecha("");
                                }
                                else
                                {
                                    dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                    date = row.getCell(b).getDateCellValue(); 
                                    sale.setFecha(dateFormat.format(date));
                                }
                                break;
                                case 1: if(StringUtils.isBlank(cellValue)){sale.setPedidoAdicional("");}else{sale.setPedidoAdicional(cellValue);} break;
                                case 2: if(StringUtils.isBlank(cellValue)){sale.setFactura("");}else{sale.setFactura(cellValue);} break;
                                case 3: if(StringUtils.isBlank(cellValue)){sale.setFolio("");}else{sale.setFolio(cellValue);} break;
                                case 4: if(StringUtils.isBlank(cellValue)){sale.setSolicitante("");}else{sale.setSolicitante(cellValue);} break;
                                case 5: if(StringUtils.isBlank(cellValue)){sale.setCedis("");}else{sale.setCedis(cellValue);} break;
                                case 6: if(StringUtils.isBlank(cellValue)){sale.setDestinatario("");}else{sale.setDestinatario(cellValue);} break;
                                case 7: if(StringUtils.isBlank(cellValue)){sale.setNombreDestinatario("");}else{sale.setNombreDestinatario(cellValue);} break;
                                case 8: if(StringUtils.isBlank(cellValue)){sale.setFacturaRemisionSicav("");}else{sale.setFacturaRemisionSicav(cellValue);} break; 
                                case 9: if(StringUtils.isBlank(cellValue)){ sale.setImporte(null); } else { sale.setImporte(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 10: if(StringUtils.isBlank(cellValue)){sale.setCliente("");}else{sale.setCliente(cellValue);} break;
                                case 11: if(StringUtils.isBlank(cellValue)){sale.setRefFact("");}else{sale.setRefFact(cellValue);} break;
                                case 12: if(StringUtils.isBlank(cellValue)){sale.setReferencia("");}else{sale.setReferencia(cellValue);}  break;
                                case 13: if(StringUtils.isBlank(cellValue)){sale.setClvRef2("");}else{sale.setClvRef2(cellValue);} break;
                                case 14: if(StringUtils.isBlank(cellValue)){sale.setClvRef3("");}else{sale.setClvRef3(cellValue);} break;
                                case 15: if(StringUtils.isBlank(cellValue)){ sale.setFechaDoc("");} else { dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                date = row.getCell(b).getDateCellValue();
                                sale.setFechaDoc(dateFormat.format(date));
                                }
                                break;
                                case 16: if(StringUtils.isBlank(cellValue)){sale.setVencNeto("");}else{sale.setVencNeto(cellValue);} break;
                                case 17: if(StringUtils.isBlank(cellValue)) { sale.setImpteMl(""); } else { sale.setImpteMl(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 18: if(StringUtils.isBlank(cellValue)){sale.setCe("");}else{sale.setCe(cellValue);} break;
                                case 19: if(StringUtils.isBlank(cellValue)){sale.setDiv("");}else{sale.setDiv(cellValue);} break;
                                default: break;
                            }
                        }
                        databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, sale); 
                    }
                    else if(maxColumnNumberRow == 20 && checkSaleColumns == false)
                    {
                        checkSaleColumns = true;
                    }
                }
            }*/
            server.info("Insert Data Information into ventas Table was sucessful");
            server.info("Close Excel File");
            book.close();
            
        }
        
        public void processLink() throws Exception
        {
            //int number1 = 0; 
            //int number2 = 0;
            int idFolio = 0; 
            FileInputStream inputFile = null;
            Workbook book;
            Sheet linkSheet;
            Row rowColumn = null; 
            Document document = new Document(); 
            document.setNumeroFolio(folioNumber);
            document.setNombre(fileName);
            Iterator<Row> iterator1 = null;
            Iterator<Row> iterator2 = null;
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(excelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Get Sheet from Excel File");
            linkSheet = book.createSheet("Punto 10");
            String[] columnNames = new String[]{"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", " ", "Pedido Adicional", "CR Tienda", "Num de Remisión", "Fecha", "Neto", " ", "Diferencia", "%", " ", "Destinatario", " Tipo de Busqueda", "Tipo de Busqueda"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = linkSheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + linkSheet.getSheetName());
            listTenPoint = databaseUtilities.searchByStoreDateAmount(server, databasePath, idFolio);
            server.info("Total de Link Elementos: " + listTenPoint.size());
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Datos del Punto 10");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
            
            for(Link link: listTenPoint)
            {
                rowCount++;
                Row row = linkSheet.createRow(rowCount);
                columnCount = 0;
                for(String columnNameValue: columnNames)
                {
                    Cell cell = row.createCell(columnCount);
                    switch(columnCount)
                    {
                        case 0: if(StringUtils.isBlank(link.getVenta().getFecha())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getFecha());} columnCount++; break; 
                        case 1: if(StringUtils.isBlank(link.getVenta().getPedidoAdicional())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getPedidoAdicional());} columnCount++; break;
                        case 2: if(StringUtils.isBlank(link.getVenta().getFactura())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getFactura());} columnCount++; break;
                        case 3: if(StringUtils.isBlank(link.getVenta().getFolio())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getFolio());} columnCount++; break;
                        case 4: if(StringUtils.isBlank(link.getVenta().getSolicitante())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getSolicitante());} columnCount++; break;
                        case 5: if(StringUtils.isBlank(link.getVenta().getCedis())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getCedis());} columnCount++; break;
                        case 6: if(StringUtils.isBlank(link.getVenta().getDestinatario())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getDestinatario());} columnCount++; break;
                        case 7: if(StringUtils.isBlank(link.getVenta().getNombreDestinatario())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getNombreDestinatario());} columnCount++; break;
                        case 8: if(StringUtils.isBlank(link.getAbreviacionVenta())){cell.setCellValue("");}else{cell.setCellValue(link.getAbreviacionVenta());} columnCount++; break;
                        case 9: if(StringUtils.isBlank(link.getVenta().getFacturaRemisionSicav())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getFacturaRemisionSicav());} columnCount++; break;
                        case 10: if(StringUtils.isBlank(link.getVenta().getImporte())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getImporte());} columnCount++; break;
                        case 11: cell.setCellValue(""); columnCount++; break;
                        case 12: if(StringUtils.isBlank(link.getRecepcion().getAdicional())){cell.setCellValue("");}else{cell.setCellValue(link.getRecepcion().getAdicional());} columnCount++; break;
                        case 13: if(StringUtils.isBlank(link.getRecepcion().getTienda())){cell.setCellValue("");}else{cell.setCellValue(link.getRecepcion().getTienda());} columnCount++; break;
                        case 14: if(StringUtils.isBlank(link.getRecepcion().getRemision())){cell.setCellValue("");}else{cell.setCellValue(link.getRecepcion().getRemision());} columnCount++; break;
                        case 15: if(StringUtils.isBlank(link.getRecepcion().getFecha())){cell.setCellValue("");}else{cell.setCellValue(link.getRecepcion().getFecha());} columnCount++; break;
                        case 16: if(StringUtils.isBlank(link.getRecepcion().getNeto())){cell.setCellValue("");}else{cell.setCellValue(link.getRecepcion().getNeto());} columnCount++; break;
                        case 17: cell.setCellValue(""); columnCount++; break;
                        case 18: if(StringUtils.isBlank(link.getDiferencia())){cell.setCellValue("");}else{cell.setCellValue(link.getDiferencia());} columnCount++; break; 
                        case 19: if(StringUtils.isBlank(link.getPorcentaje())){cell.setCellValue("");}else{cell.setCellValue(link.getPorcentaje());} columnCount++; break;
                        case 20: cell.setCellValue(""); columnCount++; break;
                        case 21: if(StringUtils.isBlank(link.getVenta().getDestinatario())){cell.setCellValue("");}else{cell.setCellValue(link.getVenta().getDestinatario());} columnCount++; break;
                        case 22: cell.setCellValue(""); columnCount++; break;
                        case 23: if(StringUtils.isBlank(link.getBusqueda())){cell.setCellValue("");}else{cell.setCellValue(link.getBusqueda());} columnCount++; break;
                        default: break; 
                    }
                }
            }
            server.info("Añadiendolos al archivo Excel");
            FileOutputStream outputStream = new FileOutputStream(excelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel"); 
        }
        
        /*public String hasMoreLink() throws Exception
        {
            return currentLinkIndex < listTenPoint.size() ? "yes" : "no"; 
        }*/
        
	/**
	 * Action "openNotepad"
	 * @return
	 * @throws Exception
	 */
	public void openNotepad() throws Exception {
		
		windows.pause(PAUSE);

		// Win+R
		windows.typeText(windows.getKeyboardSequence()
				.pressWindows().type("r").releaseWindows());
		
		// we write "notepad"
		windows.typeText("notepad");
		
		// press return key using IKeyboardSequence
		windows.typeText(windows.getKeyboardSequence().typeReturn());
		
		windows.showWindow(windows.getWindow(".*(notepad|bloc).*").gethWnd(), EShowWindowState.SW_MAXIMIZE);

		// we do an explicit pause
		windows.pause(PAUSE);
	}
	
	/**
	 * Action "processItem"
	 * @return
	 * @throws Exception
	 */
	public void processItem() throws Exception {

		String item = "this is a test";

		// we inform that we are beginning the processing of an item
		server.setCurrentItem(currentItem, item);
		
		// we write the text into notepad
		windows.typeText(item);
		
		// press return key, we use IKeyboard instead of IKeyboardSequence
		windows.keyboard().enter();

		// we inform the item processing result
		server.setCurrentItemResultToOK();
		
		// we save the screenshot, it can be viewed in robot execution trace page on the console
		server.sendScreen(String.format("Snapshot over %s", item));
	}
	
	/**
	 * Action "moreData"
	 * @return
	 * @throws Exception
	 */
	public String moreData() throws Exception {
		return "no";
	}
	
	/**
	 * Action "closeNotepad"
	 * @return
	 * @throws Exception
	 */
	public void closeNotepad() throws Exception {
		
		// press alt+F4 + pause + "n"
		windows.typeText(windows.getKeyboardSequence().typeAltF(4).pause().type("n"));
		
		// another form
		//windows.keyboard().altF(4).pause().type("n");
	}
	
	/**
	 * Action "end"
	 * @return
	 * @throws Exception
	 */
	public void end() throws Exception {
	}
	
	/**
	 * @see com.novayre.jidoka.client.api.IRobot#cleanUp()
	 */
	/*
	@Override
	public String[] cleanUp() throws Exception {
		return new String[0];
	}
	*/
        @Override
	public String[] cleanUp() throws Exception {
		return new String[] { excelFilePath };
	}
}
