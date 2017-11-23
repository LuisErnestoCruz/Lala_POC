package com.dobleo.rpa.lala;

import com.dobleo.rpa.database.DatabaseUtilities;
import com.dobleo.rpa.models.Document;
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
        private DatabaseUtilities databaseUtilities;
	private String databasePath;
        private String excelFilePath;
        private static final String PARAM_INPUT_FILE = "inputFile";
        private static final String DATABASE_NAME = "grupolala";
        private static final String TABLE_DOCUMENTS = "documento";
        private static final String TABLE_RECEPTIONS = "recepciones";
        private static final String TABLE_SALES = "ventas";
        private Map<Integer, String> documentColumns;
        private Map<Integer, String> receptionsColumns;
        private Map<Integer, String> salesColumns;
        
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
            Workbook book = WorkbookFactory.create(in);
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
                        
                        //listReception.add(reception);
                        databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                    }
                }

            }
            server.info("Read Sheet " + folioNumber + "Excel File Successfully"); 
            
            server.info("Add Reception Information into Database"); 
            /*if(listReception != null)
            {
                if(listReception.size() > 0)
                {
                    for(int b = 0; b < listReception.size(); b++)
                    {
                        databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, listReception.get(b)); 
                    }
                }
            }*/
            
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
                        //listSale.add(sale);
                    }
                    else if(maxColumnNumber == 20 && columnNameSale == false)
                    {
                        columnNameSale = true; 
                    }
                }

            }
            
            server.info("Read Sheet Venta Excel File Successfully"); 
            
            server.info("Add Sale Information into Database"); 
            /*if(listSale != null)
            {
                if(listSale.size() > 0)
                {
                    for(int d = 0; d < listSale.size(); d++)
                    {
                        databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, listSale.get(d)); 
                    }
                }
            }*/
            
            server.info("Read Complete Excel File was Successfull");
            
            //System.out.println("Creado para ambos formatos");
        }
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
}
