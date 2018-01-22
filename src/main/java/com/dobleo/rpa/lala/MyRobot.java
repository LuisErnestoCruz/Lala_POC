package com.dobleo.rpa.lala;

import com.dobleo.rpa.database.DatabaseUtilities;
import com.dobleo.rpa.models.Branch;
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
import java.util.HashSet;
import org.apache.poi.ss.usermodel.CellType;

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
        private boolean existLaterInputFile = false;
        private boolean existPastInputFile = false;
	private int currentItem = 1;
        private int currentLinkIndex = 0;
        private DatabaseUtilities databaseUtilities;
	private String databasePath;
        private String branchExcelFilePath;
        private String currentExcelFilePath;
        private String pastExcelFilePath;
        private String laterExcelFilePath;
        private String folioNumber;
        private String pastFolioNumber;
        private String laterFolioNumber;
        private String branchFileName;
        private String currentfileName;
        private String pastFileName;
        private String laterFileName;
        private String incidencePercentage;
        private static final String PARAM_BRANCH_INPUT_FILE = "sucursales";
        private static final String PARAM_CURRENT_INPUT_FILE = "folioBase";
        private static final String PARAM_PAST_INPUT_FILE = "folioPasado"; 
        private static final String PARAM_LATER_INPUT_FILE = "folioPosterior";
        private static final String PARAM_INCIDENCE_PERCENTAGE = "porcentajeIncidencia" ;
        private static final String DATABASE_NAME = "grupolala";
        private static final String TABLE_DOCUMENTS = "documento";
        private static final String TABLE_RECEPTIONS = "recepciones";
        private static final String TABLE_SALES = "ventas";
        private static final String TABLE_BRANCH = "sucursales"; 
        private static final String TABLE_VIRTUAL_RECEPTIONS = "virtual_recepciones"; 
        private static final String TABLE_VIRTUAL_SALES = "virtual_ventas";
        private static final String TABLE_VIRTUALS_MOORAGE = "virtual_amarre"; 
        private Map<Integer, String> documentColumns;
        private Map<Integer, String> receptionsColumns;
        private Map<Integer, String> virtualReceptionsColumns;
        private Map<Integer, String> virtualSalesColumns;
        private Map<Integer, String> virtualMoorageColumns;
        private Map<Integer, String> salesColumns;
        private Map<Integer, String> branchsColumns;
        private ArrayList<Link> listFirstJoin; 
        private ArrayList<Link> listTenPoint;
        private ArrayList<Integer> listIdReceptionNotMatch; 
        private ArrayList<Integer> listRemoveIdReceptionNotMatch;
        private ArrayList<Integer> listIdSaleNotMatch;
        private ArrayList<Integer> listRemoveIdSaleNotMatch;
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
                virtualReceptionsColumns = null;
                virtualSalesColumns = null;
                virtualMoorageColumns = null;
                salesColumns = null;
                databasePath = "";
                listIdReceptionNotMatch = new ArrayList<Integer>();
                listRemoveIdReceptionNotMatch = new ArrayList<Integer>();
                listIdSaleNotMatch = new ArrayList<Integer>();
                listRemoveIdSaleNotMatch = new ArrayList<Integer>();
                branchFileName = server.getParameters().get(PARAM_BRANCH_INPUT_FILE);
                currentfileName = server.getParameters().get(PARAM_CURRENT_INPUT_FILE);
                pastFileName = server.getParameters().get(PARAM_PAST_INPUT_FILE);
                laterFileName = server.getParameters().get(PARAM_LATER_INPUT_FILE);
                branchExcelFilePath = Paths.get(server.getCurrentDir(), branchFileName).toString();
                currentExcelFilePath = Paths.get(server.getCurrentDir(), currentfileName).toString();
                //pastExcelFilePath = Paths.get(server.getCurrentDir(), pastFileName).toString();
                incidencePercentage = server.getParameters().get(PARAM_INCIDENCE_PERCENTAGE);                
                
                if(pastFileName != null)
                {
                    if(StringUtils.isBlank(pastFileName) == false && StringUtils.isEmpty(pastFileName) == false)
                    {
                        pastExcelFilePath = Paths.get(server.getCurrentDir(), pastFileName).toString();
                        existPastInputFile = true;
                    }
                }
                
                if(laterFileName != null)
                {
                    if(StringUtils.isBlank(laterFileName) == false && StringUtils.isEmpty(laterFileName) == false)
                    {
                        laterExcelFilePath = Paths.get(server.getCurrentDir(), laterFileName).toString();
                        existLaterInputFile = true;
                    }
                }
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
            server.info("Initialization Virtual recepciones Table Columns");
            databaseUtilities.virtualReceptionTableColumns();
            virtualReceptionsColumns = databaseUtilities.getVirtualReceptionsColumns();
            server.info("Initialization ventas Table Columns");
            databaseUtilities.saleTableColumns();
            salesColumns = databaseUtilities.getSalesColumns();
            server.info("Initialization sucursales Table Columns");
            databaseUtilities.branchTableColumns();
            branchsColumns = databaseUtilities.getBranchColumns(); 
            server.info("Initialization Virtual ventas Table Columns");
            databaseUtilities.virtualSaleTableColumns();
            virtualSalesColumns = databaseUtilities.getVirtualSalesColumns();
            server.info("Initialization Virtual amarre Table Columns");
            databaseUtilities.virtualTableColumns();
            virtualMoorageColumns = databaseUtilities.getVirtualTableColumns();
            databaseUtilities.createTable(server, databasePath, TABLE_DOCUMENTS, documentColumns);
            databaseUtilities.createTable(server, databasePath, TABLE_RECEPTIONS, receptionsColumns);
            //databaseUtilities.createVirtualTable(server, databasePath, TABLE_VIRTUAL_RECEPTIONS, virtualReceptionsColumns);
            databaseUtilities.createTable(server, databasePath, TABLE_SALES, salesColumns);
            databaseUtilities.createTable(server, databasePath, TABLE_BRANCH, branchsColumns); 
            //databaseUtilities.createVirtualTable(server, databasePath, TABLE_VIRTUAL_SALES, virtualSalesColumns);
            //databaseUtilities.createVirtualTable(server, databasePath, TABLE_VIRTUALS_MOORAGE, virtualMoorageColumns);
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
            String fileName = server.getParameters().get(PARAM_CURRENT_INPUT_FILE);
            server.info("Obtain Excel File Path");
            currentExcelFilePath = Paths.get(server.getCurrentDir(), fileName).toString();
            server.info("Excel File Path: " + currentExcelFilePath);
            FileInputStream in =new FileInputStream(new File(currentExcelFilePath));
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
                        //databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
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
                        
                        //databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, sale); 
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
        
        public void readExcelBranch() throws Exception
        {   
            int times = 0;
            int columnNumberRow = 0;
            int maxColumnNumberRow = 0;
            int totalCellBlank = 0;
            int totalColumn = 0;
            int columnIndex = 0;
            ArrayList<Branch> listBranch = new ArrayList<Branch>(); 
            String cellValue = null;
            String sucursalLALA = "";
            String sucursalOXXO = ""; 
            Reception reception = new Reception();
            Branch branch = new Branch(); 
            Workbook book = null;
            //Row row = null;
            Sheet branchSheet = null;
            InputStream inputFile = null;
            Iterator<Cell> iteratorCell = null;
            Date date = null;
            DataFormatter dataFormatter = new DataFormatter();
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(branchExcelFilePath));
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Sheet from Excel File");
            branchSheet = book.getSheetAt(0);
            server.info("Read All Filled Row From Sheet: " + branchSheet.getSheetName()); 
            server.info("Insert Data Information Into sucursales Table");
            
            for(Row row: branchSheet)
            {
                if(row == null)
                {
                    break;
                }
                
                columnNumberRow = row.getLastCellNum();

                totalCellBlank = 0;
                iteratorCell = null;
                branch = null;
                branch = new Branch();
                
                if(columnNumberRow > 16)
                {
                    totalColumn = columnNumberRow - 3;
                }
                else 
                {
                    totalColumn = columnNumberRow;
                }
                
                if(totalColumn != 16)
                {
                    server.info("Valor es: " + totalColumn); 
                }
                
                iteratorCell = row.cellIterator();

                

                if(totalColumn == 16 && times > 1)
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
                                case 0: if(StringUtils.isBlank(cellValue)){branch.setAnalista("");}else{branch.setAnalista(cellValue);} columnIndex++; break;
                                case 1: if(StringUtils.isBlank(cellValue)){branch.setCliente("");}else{branch.setCliente(cellValue);} columnIndex++; break;
                                case 2: if(StringUtils.isBlank(cellValue)){branch.setZona("");}else{branch.setZona(cellValue);} columnIndex++; break;
                                case 3: if(StringUtils.isBlank(cellValue)){branch.setCentro("");}else{branch.setCentro(cellValue);} columnIndex++; break;
                                case 4: if(StringUtils.isBlank(cellValue)){branch.setCedis("");}else{branch.setCedis(cellValue);} columnIndex++; break;
                                case 5: if(StringUtils.isBlank(cellValue)){branch.setSucursalSAP("");}else{branch.setSucursalSAP(cellValue);} columnIndex++; break;
                                case 6: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        branch.setSucursalLALA(""); 
                                        branch.setSucursalLALA2("");}
                                    else
                                    {
                                        branch.setSucursalLALA(cellValue); 
                                        
                                        sucursalLALA = ""; 
                                        sucursalLALA = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                        sucursalLALA = sucursalLALA.replace(" ", "");
                                        sucursalLALA = StringUtils.stripAccents(sucursalLALA); 
                                        sucursalLALA = sucursalLALA.replaceAll("[^a-zA-Z0-9]+", "");
                                        sucursalLALA = sucursalLALA.toLowerCase();
                                        branch.setSucursalLALA2(sucursalLALA);
                                    } 
                                    columnIndex++; 
                                    break;
                                case 7: columnIndex++; break;
                                case 8: if(StringUtils.isBlank(cellValue)){branch.setCrLALA("");}else{branch.setCrLALA(cellValue);} columnIndex++; break;
                                case 9: if(StringUtils.isBlank(cellValue)){branch.setPlaza1("");}else{branch.setPlaza1(cellValue);} columnIndex++; break; 
                                case 10: if(StringUtils.isBlank(cellValue)){branch.setCrOXXO("");}else{branch.setCrOXXO(cellValue);} columnIndex++; break;
                                case 11: if(StringUtils.isBlank(cellValue)){branch.setPlaza2("");}else{branch.setPlaza2(cellValue);} columnIndex++; break;
                                case 12: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        branch.setSucursalOXXO("");
                                        branch.setSucursalOXXO2("");
                                    }
                                    else
                                    {
                                        branch.setSucursalOXXO(cellValue);
                                        sucursalOXXO = ""; 
                                        sucursalOXXO = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                        sucursalOXXO = sucursalOXXO.replace(" ", "");
                                        sucursalOXXO = StringUtils.stripAccents(sucursalOXXO); 
                                        sucursalOXXO = sucursalOXXO.replaceAll("[^a-zA-Z0-9]+", "");
                                        sucursalOXXO = sucursalOXXO.toLowerCase();
                                        branch.setSucursalOXXO2(sucursalOXXO);
                                    } 
                                    columnIndex++; 
                                    break;
                                case 13: if(StringUtils.isBlank(cellValue)){branch.setLiquidacion("");}else{branch.setLiquidacion(cellValue);} columnIndex++; break;
                                case 14: columnIndex++; break;
                                case 15: if(StringUtils.isBlank(cellValue)){branch.setVentaCruzada("");}else{branch.setVentaCruzada(cellValue);} columnIndex++; break;
                                default: break;
                            }
                        //}

                    }
                    listBranch.add(branch);
                    //databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                }
                
                times = times + 1;


            }
            databaseUtilities.insertIntoBranch(server, databasePath, TABLE_BRANCH, branchsColumns, listBranch);
            server.info("Insert Data Information into sucursales Table was sucessful");
            server.info("Close Excel File"); 
            book.close();
        }
        
        
        public void readExcelFolioSheet() throws Exception
        {
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            double percentage = 0.0;
            double operation = 0.0;
            folioNumber = null;
            currentfileName = null;
            Document document = new Document(); 
            Workbook book = null;
            server.info("Open File Excel"); 
            InputStream inputFile = new FileInputStream(new File(currentExcelFilePath));
            server.info("Get input File Information");
            currentfileName = server.getParameters().get(PARAM_CURRENT_INPUT_FILE);
            server.info("Obtain Excel File Path");
            currentExcelFilePath = Paths.get(server.getCurrentDir(), currentfileName).toString();
            server.info("Excel File Path: " + currentExcelFilePath);
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Name from Second Sheet into Excel");
            folioNumber = book.getSheetName(1);
            server.info("Name of Second Sheet is: " + folioNumber);
            server.info("Set information to Document object");
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            //document.setPorcentajeIncidencia("0.05");
            if(incidencePercentage != null)
            {
                if(StringUtils.isBlank(incidencePercentage) == false && StringUtils.isEmpty(incidencePercentage) == false)
                {
                    percentage = Double.parseDouble(incidencePercentage);
                    operation = percentage / (double)100;
                    document.setPorcentajeIncidencia(decimalFormat.format(operation));
                }
                else
                {
                    document.setPorcentajeIncidencia("0.05");
                }
            }
            else 
            {
                document.setPorcentajeIncidencia("0.05");
            }
            
            server.info("Insert into documento Table Information");
            databaseUtilities.insertIntoDocument(server, databasePath, TABLE_DOCUMENTS, documentColumns, document);
            server.info("Insert into documento Table was successful");
            server.info("Close Excel File"); 
            book.close();
        }
        
        public void readExcelPastFolioSheet() throws Exception
        {
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            double percentage = 0.0;
            double operation = 0.0;
            pastFileName = null;
            Document document = new Document(); 
            Workbook book = null;
            server.info("Open File Excel"); 
            InputStream inputFile = new FileInputStream(new File(pastExcelFilePath));
            server.info("Get input File Information");
            pastFileName = server.getParameters().get(PARAM_PAST_INPUT_FILE);
            server.info("Obtain Excel File Path");
            pastExcelFilePath = Paths.get(server.getCurrentDir(), pastFileName).toString();
            server.info("Excel File Path: " + pastExcelFilePath);
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Name from Second Sheet into Excel");
            pastFolioNumber = book.getSheetName(1);
            server.info("Name of Second Sheet is: " + pastFolioNumber);
            server.info("Set information to Document object");
            document.setNumeroFolio(pastFolioNumber);
            document.setNombre(pastFileName);
            if(incidencePercentage != null)
            {
                if(StringUtils.isBlank(incidencePercentage) == false && StringUtils.isEmpty(incidencePercentage) == false)
                {
                    percentage = Double.parseDouble(incidencePercentage);
                    operation = percentage / (double)100;
                    document.setPorcentajeIncidencia(decimalFormat.format(operation));
                }
                else 
                {
                    document.setPorcentajeIncidencia("0.05");
                }
            }
            else
            {
                document.setPorcentajeIncidencia("0.05");
            }
            server.info("Insert into documento Table Information");
            databaseUtilities.insertIntoDocument(server, databasePath, TABLE_DOCUMENTS, documentColumns, document);
            server.info("Insert into documento Table was successful");
            server.info("Close Excel File"); 
            book.close();
        }
        
        public void readExcelLaterFolioSheet() throws Exception
        {
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            double percentage = 0.0;
            double operation = 0.0;
            laterFileName = null;
            Document document = new Document(); 
            Workbook book = null;
            server.info("Open File Excel"); 
            InputStream inputFile = new FileInputStream(new File(laterExcelFilePath));
            server.info("Get input File Information");
            laterFileName = server.getParameters().get(PARAM_LATER_INPUT_FILE);
            server.info("Obtain Excel File Path");
            laterExcelFilePath = Paths.get(server.getCurrentDir(), laterFileName).toString();
            server.info("Excel File Path: " + laterExcelFilePath);
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Name from Second Sheet into Excel");
            laterFolioNumber = book.getSheetName(1);
            server.info("Name of Second Sheet is: " + laterFolioNumber);
            server.info("Set information to Document object");
            document.setNumeroFolio(laterFolioNumber);
            document.setNombre(laterFileName);
            if(incidencePercentage != null)
            {
                if(StringUtils.isBlank(incidencePercentage) == false && StringUtils.isEmpty(incidencePercentage) == false)
                {
                    percentage = Double.parseDouble(incidencePercentage);
                    operation = percentage / (double)100;
                    document.setPorcentajeIncidencia(decimalFormat.format(operation));
                }
                else
                {
                    document.setPorcentajeIncidencia("0.05");
                }
            }
            else 
            {
                document.setPorcentajeIncidencia("0.05");
            }
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
            ArrayList<Reception> listReception = new ArrayList<Reception>(); 
            String cellValue = null;
            String tienda2 = null;
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
            document.setNombre(currentfileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
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
                                    case 1: 
                                        if(StringUtils.isBlank(cellValue))
                                        {
                                            reception.setTienda("");
                                            reception.setTienda2("");
                                        }
                                        else
                                        {
                                            reception.setTienda(cellValue);
                                            tienda2 = ""; 
                                            tienda2 = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                            tienda2 = tienda2.replace(" ", "");
                                            tienda2 = StringUtils.stripAccents(tienda2); 
                                            tienda2 = tienda2.replaceAll("[^a-zA-Z0-9]+", "");
                                            tienda2 = tienda2.toLowerCase();
                                            reception.setTienda2(tienda2);
                                        } 
                                        columnIndex++; break;
                                    case 2: if(StringUtils.isBlank(cellValue)){reception.setRecibo("");}else{reception.setRecibo(cellValue);} columnIndex++; break;
                                    case 3: if(StringUtils.isBlank(cellValue)){reception.setOrden("");}else{reception.setOrden(cellValue);} columnIndex++; break;
                                    case 4: if(StringUtils.isBlank(cellValue)){reception.setAdicional("");}else{reception.setAdicional(cellValue);} columnIndex++; break;
                                    case 5: if(StringUtils.isBlank(cellValue)){reception.setRemision("");}else{reception.setRemision(cellValue);} columnIndex++; break;
                                    case 6: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        reception.setFecha("");
                                        reception.setFecha2("");
                                    }
                                    else
                                    {
                                        date = row.getCell(b).getDateCellValue(); 
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        reception.setFecha(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        reception.setFecha2(dateFormat.format(date));
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
                        listReception.add(reception);
                        //databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                    }
                    
                    
                }
            }
            databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, listReception);
            //databaseUtilities.insertIntoVirtualReception(server, databasePath, TABLE_VIRTUAL_RECEPTIONS, virtualReceptionsColumns, listReception); 
            server.info("Insert Data Information into recepciones Table was sucessful");
            server.info("Close Excel File"); 
            book.close();
            
        }
        
        public void readExcelPastReceptionSheet() throws Exception
        {
            int idFolio = 0;
            int columnNumberRow = 0;
            int maxColumnNumberRow = 0;
            int totalCellBlank = 0;
            int totalColumn = 0;
            int columnIndex = 0;
            ArrayList<Reception> listReception = new ArrayList<Reception>(); 
            String cellValue = null;
            String tienda2 = null;
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
            document.setNumeroFolio(pastFolioNumber);
            document.setNombre(pastFileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(pastExcelFilePath));
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Sheet from Excel File");
            receptionSheet = book.getSheetAt(1);
            server.info("Read All Filled Row From Sheet: " + receptionSheet.getSheetName()); 
            server.info("Insert Data Information Into recepciones Table");
            
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
                                    case 1: 
                                        if(StringUtils.isBlank(cellValue))
                                        {
                                            reception.setTienda("");
                                            reception.setTienda2(tienda2);
                                        }
                                        else
                                        {
                                            reception.setTienda(cellValue);
                                            tienda2 = ""; 
                                            tienda2 = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                            tienda2 = tienda2.replace(" ", "");
                                            tienda2 = StringUtils.stripAccents(tienda2); 
                                            tienda2 = tienda2.replaceAll("[^a-zA-Z0-9]+", "");
                                            tienda2 = tienda2.toLowerCase();
                                            reception.setTienda2(tienda2);
                                        } 
                                        columnIndex++; 
                                        break;
                                    case 2: if(StringUtils.isBlank(cellValue)){reception.setRecibo("");}else{reception.setRecibo(cellValue);} columnIndex++; break;
                                    case 3: if(StringUtils.isBlank(cellValue)){reception.setOrden("");}else{reception.setOrden(cellValue);} columnIndex++; break;
                                    case 4: if(StringUtils.isBlank(cellValue)){reception.setAdicional("");}else{reception.setAdicional(cellValue);} columnIndex++; break;
                                    case 5: if(StringUtils.isBlank(cellValue)){reception.setRemision("");}else{reception.setRemision(cellValue);} columnIndex++; break;
                                    case 6: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        reception.setFecha("");
                                        reception.setFecha2("");
                                    }
                                    else
                                    {
                                        date = row.getCell(b).getDateCellValue(); 
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        reception.setFecha(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        reception.setFecha2(dateFormat.format(date));
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
                        listReception.add(reception);
                        //databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                    }
                    
                    
                }
            }
            databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, listReception);
            //databaseUtilities.insertIntoVirtualReception(server, databasePath, TABLE_VIRTUAL_RECEPTIONS, virtualReceptionsColumns, listReception); 
            server.info("Insert Data Information into recepciones Table was sucessful");
            server.info("Close Excel File"); 
            book.close();
            
        }
        
        public void readExcelLaterReceptionSheet() throws Exception
        {
            int idFolio = 0;
            int columnNumberRow = 0;
            int maxColumnNumberRow = 0;
            int totalCellBlank = 0;
            int totalColumn = 0;
            int columnIndex = 0;
            ArrayList<Reception> listReception = new ArrayList<Reception>(); 
            String cellValue = null;
            String tienda2 = null;
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
            document.setNumeroFolio(laterFolioNumber);
            document.setNombre(laterFileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(laterExcelFilePath));
            server.info("Obtain Workbook from Excel File");
            //book = WorkbookFactory.create(inputFile);
            book = StreamingReader.builder().open(inputFile);
            server.info("Get Sheet from Excel File");
            receptionSheet = book.getSheetAt(1);
            server.info("Read All Filled Row From Sheet: " + receptionSheet.getSheetName()); 
            server.info("Insert Data Information Into recepciones Table");
            
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
                                    case 1: 
                                        if(StringUtils.isBlank(cellValue))
                                        {
                                            reception.setTienda("");
                                            reception.setTienda2(tienda2);
                                        }
                                        else
                                        {
                                            reception.setTienda(cellValue);
                                            tienda2 = ""; 
                                            tienda2 = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                            tienda2 = tienda2.replace(" ", "");
                                            tienda2 = StringUtils.stripAccents(tienda2); 
                                            tienda2 = tienda2.replaceAll("[^a-zA-Z0-9]+", "");
                                            tienda2 = tienda2.toLowerCase();
                                            reception.setTienda2(tienda2);
                                        } 
                                        columnIndex++; 
                                        break;
                                    case 2: if(StringUtils.isBlank(cellValue)){reception.setRecibo("");}else{reception.setRecibo(cellValue);} columnIndex++; break;
                                    case 3: if(StringUtils.isBlank(cellValue)){reception.setOrden("");}else{reception.setOrden(cellValue);} columnIndex++; break;
                                    case 4: if(StringUtils.isBlank(cellValue)){reception.setAdicional("");}else{reception.setAdicional(cellValue);} columnIndex++; break;
                                    case 5: if(StringUtils.isBlank(cellValue)){reception.setRemision("");}else{reception.setRemision(cellValue);} columnIndex++; break;
                                    case 6: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        reception.setFecha("");
                                        reception.setFecha2("");
                                    }
                                    else
                                    {
                                        date = row.getCell(b).getDateCellValue(); 
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        reception.setFecha(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        reception.setFecha2(dateFormat.format(date));
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
                        listReception.add(reception);
                        //databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, reception);
                    }
                    
                    
                }
            }
            databaseUtilities.insertIntoReception(server, databasePath, TABLE_RECEPTIONS, receptionsColumns, listReception);
            //databaseUtilities.insertIntoVirtualReception(server, databasePath, TABLE_VIRTUAL_RECEPTIONS, virtualReceptionsColumns, listReception); 
            server.info("Insert Data Information into recepciones Table was sucessful");
            server.info("Close Excel File"); 
            book.close();
            
        }
        
        public void readExcelSaleSheet() throws Exception
        {
            boolean checkSaleColumns = false;
            int idFolio = 0;
            int cellTypeNumber = 0;
            short columnNumberRow = 0;
            short maxColumnNumberRow = 0; 
            String cellValue = null;
            String nombreDestinatario2 = null;
            Document document = new Document();
            Sale sale = new Sale();
            Workbook book = null;
            Sheet saleSheet = null;
            Row row = null; 
            Cell cell = null; 
            InputStream inputFile = null;
            SimpleDateFormat dateFormat = null;
            Date date = null;
            DataFormatter dataFormatter = new DataFormatter();
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            ArrayList<Sale> listSale = new ArrayList<Sale>();
            server.info("Set Information into document Object");
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
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
                            cell =  row.getCell(0);
                            if(cell != null)
                            {
                                try 
                                {
                                    cellTypeNumber = cell.getCellType();
                                    CellType cellType = cell.getCellTypeEnum();
                                } catch (NullPointerException e) {
                                    cellTypeNumber = 3;
                                }
                                
                                if(cellTypeNumber == 3)
                                {
                                   break; 
                                }
                                
                                date = cell.getDateCellValue();
                                if(date == null)
                                {
                                    break;
                                }
                                else
                                {
                                    dataFormatter = new DataFormatter();
                                    cellValue = dataFormatter.formatCellValue(cell);
                                    cellValue = cellValue.replace(" ", "");
                                    if(StringUtils.isBlank(cellValue) || StringUtils.isEmpty(cellValue))
                                    {
                                        break;
                                    }
                                    
                                }
                            }
                            else
                            {
                               break;
                            }
                        }
                        
                        for(int b = 0; b < maxColumnNumberRow; b++)
                        {
                            if(row.getCell(b) != null)
                            {
                                if((row.getCell(b).getCellTypeEnum() != CellType.BLANK) || (row.getCell(b).getCellTypeEnum() != CellType._NONE))
                                {
                                    //server.info("Tipo de Celda: " + row.getCell(b).getCellType());
                                    dataFormatter = new DataFormatter();
                                    cellValue = dataFormatter.formatCellValue(row.getCell(b));
                                }
                            }
                            else 
                            {
                                cellValue = "";
                            }
                            
                            dataFormatter = new DataFormatter();
                            cellValue = dataFormatter.formatCellValue(row.getCell(b));
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
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        sale.setFecha2(dateFormat.format(date));
                                    }
                                break;
                                case 1: if(StringUtils.isBlank(cellValue)){sale.setPedidoAdicional("");}else{sale.setPedidoAdicional(cellValue);} break;
                                case 2: if(StringUtils.isBlank(cellValue)){sale.setFactura("");}else{sale.setFactura(cellValue);} break;
                                case 3: if(StringUtils.isBlank(cellValue)){sale.setFolio("");}else{sale.setFolio(cellValue);} break;
                                case 4: if(StringUtils.isBlank(cellValue)){sale.setSolicitante("");}else{sale.setSolicitante(cellValue);} break;
                                case 5: if(StringUtils.isBlank(cellValue)){sale.setCedis("");}else{sale.setCedis(cellValue);} break;
                                case 6: if(StringUtils.isBlank(cellValue)){sale.setDestinatario("");}else{sale.setDestinatario(cellValue);} break;
                                case 7: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        sale.setNombreDestinatario("");
                                        sale.setNombreDestinatario2("");
                                    }
                                    else
                                    {
                                        sale.setNombreDestinatario(cellValue); 
                                        nombreDestinatario2 = ""; 
                                        nombreDestinatario2 = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                        nombreDestinatario2 = nombreDestinatario2.replace(" ", "");
                                        nombreDestinatario2 = StringUtils.stripAccents(nombreDestinatario2); 
                                        nombreDestinatario2 = nombreDestinatario2.replaceAll("[^a-zA-Z0-9]+", "");
                                        nombreDestinatario2 = nombreDestinatario2.toLowerCase();
                                        sale.setNombreDestinatario2(nombreDestinatario2);
                                    }
                                break;
                                case 8: if(StringUtils.isBlank(cellValue)){sale.setFacturaRemisionSicav("");}else{sale.setFacturaRemisionSicav(cellValue);} break; 
                                case 9: if(StringUtils.isBlank(cellValue)){ sale.setImporte(""); } else { sale.setImporte(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 10: if(StringUtils.isBlank(cellValue)){sale.setCliente("");}else{sale.setCliente(cellValue);} break;
                                case 11: if(StringUtils.isBlank(cellValue)){sale.setRefFact("");}else{sale.setRefFact(cellValue);} break;
                                case 12: if(StringUtils.isBlank(cellValue)){sale.setReferencia("");}else{sale.setReferencia(cellValue);}  break;
                                case 13: if(StringUtils.isBlank(cellValue)){sale.setClvRef2("");}else{sale.setClvRef2(cellValue);} break;
                                case 14: if(StringUtils.isBlank(cellValue)){sale.setClvRef3("");}else{sale.setClvRef3(cellValue);} break;
                                case 15: 
                                    if(StringUtils.isBlank(cellValue))
                                    { 
                                        sale.setFechaDoc("");
                                        sale.setFechaDoc2("");
                                    } 
                                    else 
                                    { 
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        date = row.getCell(b).getDateCellValue();
                                        sale.setFechaDoc(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        sale.setFechaDoc2(dateFormat.format(date));
                                    }
                                    break;
                                case 16: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        sale.setVencNeto("");
                                        sale.setVencNeto2("");
                                    }
                                    else
                                    {
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy"); 
                                        date = row.getCell(b).getDateCellValue();
                                        sale.setVencNeto(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        sale.setVencNeto2(dateFormat.format(date));
                                    } 
                                    break;
                                case 17: if(StringUtils.isBlank(cellValue)) { sale.setImpteMl(""); } else { sale.setImpteMl(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 18: if(StringUtils.isBlank(cellValue)){sale.setCe("");}else{sale.setCe(cellValue);} break;
                                case 19: if(StringUtils.isBlank(cellValue)){sale.setDiv("");}else{sale.setDiv(cellValue);} break;
                                default: break;
                            }
                        }
                        listSale.add(sale); 
                        //databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, sale); 
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
                            if(row.getCell(b) != null)
                            {
                                if(row.getCell(b).getCellType() != CellType.BLANK || row.getCell(b).getCellTypeEnum() != CellType._NONE || row.getCell(b).getCellTypeEnum() != CellType.FORMULA)
                                {
                                    server.info("Tipo de Celda: " + row.getCell(b).getCellType());
                                    dataFormatter = new DataFormatter();
                                    cellValue = dataFormatter.formatCellValue(row.getCell(b));
                                    
                                }
                            }
                            else 
                            {
                                cellValue = "";
                            }
                            //server.info(cellValue); 
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
                        listSale.add(sale); 
                    }
                    else if(maxColumnNumberRow == 20 && checkSaleColumns == false)
                    {
                        checkSaleColumns = true;
                    }
                }
            }*/
            databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, listSale);
            //databaseUtilities.insertIntoVirtualSale(server, databasePath, TABLE_VIRTUAL_SALES, virtualSalesColumns, listSale);
            server.info("Insert Data Information into ventas Table was sucessful");
            server.info("Close Excel File");
            book.close();
            
        }
        
        public void readExcelPastSaleSheet() throws Exception
        {
            boolean checkSaleColumns = false;
            int idFolio = 0;
            int cellTypeNumber = 0;
            short columnNumberRow = 0;
            short maxColumnNumberRow = 0; 
            String cellValue = null;
            String nombreDestinatario2 = null;
            Document document = new Document();
            Sale sale = new Sale();
            Workbook book = null;
            Sheet saleSheet = null;
            Row row = null; 
            Cell cell = null;
            InputStream inputFile = null;
            SimpleDateFormat dateFormat = null;
            Date date = null;
            DataFormatter dataFormatter = new DataFormatter();
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            ArrayList<Sale> listSale = new ArrayList<Sale>();
            server.info("Set Information into document Object");
            document.setNumeroFolio(pastFolioNumber);
            document.setNombre(pastFileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(pastExcelFilePath));
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
                            cell =  row.getCell(0);
                            if(cell != null)
                            {
                                try 
                                {
                                    cellTypeNumber = cell.getCellType();
                                    CellType cellType = cell.getCellTypeEnum();
                                } catch (NullPointerException e) {
                                    cellTypeNumber = 3;
                                }
                                
                                if(cellTypeNumber == 3)
                                {
                                   break; 
                                }
                                
                                date = cell.getDateCellValue();
                                if(date == null)
                                {
                                    break;
                                }
                                else
                                {
                                    dataFormatter = new DataFormatter();
                                    cellValue = dataFormatter.formatCellValue(cell);
                                    cellValue = cellValue.replace(" ", "");
                                    if(StringUtils.isBlank(cellValue) || StringUtils.isEmpty(cellValue))
                                    {
                                        break;
                                    }
                                    
                                }
                            }
                            else
                            {
                               break;
                            }
                        }
                        
                        
                        
                        for(int b = 0; b < maxColumnNumberRow; b++)
                        {
                            dataFormatter = new DataFormatter();
                            cellValue = dataFormatter.formatCellValue(row.getCell(b));
                            switch(b)
                            {
                                case 0:                                 
                                if(StringUtils.isBlank(cellValue))
                                {
                                    sale.setFecha("");
                                    sale.setFecha2("");
                                }
                                else
                                {
                                    dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                    date = row.getCell(b).getDateCellValue(); 
                                    sale.setFecha(dateFormat.format(date));
                                    dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                    sale.setFecha2(dateFormat.format(date));
                                }
                                break;
                                case 1: if(StringUtils.isBlank(cellValue)){sale.setPedidoAdicional("");}else{sale.setPedidoAdicional(cellValue);} break;
                                case 2: if(StringUtils.isBlank(cellValue)){sale.setFactura("");}else{sale.setFactura(cellValue);} break;
                                case 3: if(StringUtils.isBlank(cellValue)){sale.setFolio("");}else{sale.setFolio(cellValue);} break;
                                case 4: if(StringUtils.isBlank(cellValue)){sale.setSolicitante("");}else{sale.setSolicitante(cellValue);} break;
                                case 5: if(StringUtils.isBlank(cellValue)){sale.setCedis("");}else{sale.setCedis(cellValue);} break;
                                case 6: if(StringUtils.isBlank(cellValue)){sale.setDestinatario("");}else{sale.setDestinatario(cellValue);} break;
                                case 7: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        sale.setNombreDestinatario("");
                                        sale.setNombreDestinatario2("");
                                    }
                                    else
                                    {
                                        sale.setNombreDestinatario(cellValue); 
                                        nombreDestinatario2 = ""; 
                                        nombreDestinatario2 = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                        nombreDestinatario2 = nombreDestinatario2.replace(" ", "");
                                        nombreDestinatario2 = StringUtils.stripAccents(nombreDestinatario2); 
                                        nombreDestinatario2 = nombreDestinatario2.replaceAll("[^a-zA-Z0-9]+", "");
                                        nombreDestinatario2 = nombreDestinatario2.toLowerCase();
                                        sale.setNombreDestinatario2(nombreDestinatario2);
                                    }
                                break;
                                case 8: if(StringUtils.isBlank(cellValue)){sale.setFacturaRemisionSicav("");}else{sale.setFacturaRemisionSicav(cellValue);} break; 
                                case 9: if(StringUtils.isBlank(cellValue)){ sale.setImporte(""); } else { sale.setImporte(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 10: if(StringUtils.isBlank(cellValue)){sale.setCliente("");}else{sale.setCliente(cellValue);} break;
                                case 11: if(StringUtils.isBlank(cellValue)){sale.setRefFact("");}else{sale.setRefFact(cellValue);} break;
                                case 12: if(StringUtils.isBlank(cellValue)){sale.setReferencia("");}else{sale.setReferencia(cellValue);}  break;
                                case 13: if(StringUtils.isBlank(cellValue)){sale.setClvRef2("");}else{sale.setClvRef2(cellValue);} break;
                                case 14: if(StringUtils.isBlank(cellValue)){sale.setClvRef3("");}else{sale.setClvRef3(cellValue);} break;
                                case 15: 
                                    if(StringUtils.isBlank(cellValue))
                                    { 
                                        sale.setFechaDoc("");
                                        sale.setFechaDoc2("");
                                    } 
                                    else 
                                    { 
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        date = row.getCell(b).getDateCellValue();
                                        sale.setFechaDoc(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        sale.setFechaDoc2(dateFormat.format(date));
                                    }
                                break;
                                case 16: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        sale.setVencNeto("");
                                        sale.setVencNeto2("");
                                    }
                                    else
                                    {
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        date = row.getCell(b).getDateCellValue();
                                        sale.setVencNeto(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        sale.setVencNeto2(dateFormat.format(date));
                                    }
                                    break;
                                case 17: if(StringUtils.isBlank(cellValue)) { sale.setImpteMl(""); } else { sale.setImpteMl(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 18: if(StringUtils.isBlank(cellValue)){sale.setCe("");}else{sale.setCe(cellValue);} break;
                                case 19: if(StringUtils.isBlank(cellValue)){sale.setDiv("");}else{sale.setDiv(cellValue);} break;
                                default: break;
                            }
                        }
                        listSale.add(sale);  
                    }
                    else if(maxColumnNumberRow == 20 && checkSaleColumns == false)
                    {
                        checkSaleColumns = true;
                    }
                }
            }
            databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, listSale);
            //databaseUtilities.insertIntoVirtualSale(server, databasePath, TABLE_VIRTUAL_SALES, virtualSalesColumns, listSale);
            server.info("Insert Data Information into ventas Table was sucessful");
            server.info("Close Excel File");
            book.close();
            
        }
        
        public void readExcelLaterSaleSheet() throws Exception
        {
            boolean checkSaleColumns = false;
            int idFolio = 0;
            int cellTypeNumber = 0;
            short columnNumberRow = 0;
            short maxColumnNumberRow = 0; 
            String cellValue = null;
            String nombreDestinatario2 = null;
            Document document = new Document();
            Sale sale = new Sale();
            Workbook book = null;
            Sheet saleSheet = null;
            Row row = null; 
            Cell cell = null;
            InputStream inputFile = null;
            SimpleDateFormat dateFormat = null;
            Date date = null;
            DataFormatter dataFormatter = new DataFormatter();
            DecimalFormat decimalFormat = new DecimalFormat("#.00");
            ArrayList<Sale> listSale = new ArrayList<Sale>();
            server.info("Set Information into document Object");
            document.setNumeroFolio(laterFolioNumber);
            document.setNombre(laterFileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(laterExcelFilePath));
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
                            cell =  row.getCell(0);
                            if(cell != null)
                            {
                                try 
                                {
                                    cellTypeNumber = cell.getCellType();
                                    CellType cellType = cell.getCellTypeEnum();
                                } catch (NullPointerException e) {
                                    cellTypeNumber = 3;
                                }
                                
                                if(cellTypeNumber == 3)
                                {
                                   break; 
                                }
                                
                                date = cell.getDateCellValue();
                                if(date == null)
                                {
                                    break;
                                }
                                else
                                {
                                    dataFormatter = new DataFormatter();
                                    cellValue = dataFormatter.formatCellValue(cell);
                                    cellValue = cellValue.replace(" ", "");
                                    if(StringUtils.isBlank(cellValue) || StringUtils.isEmpty(cellValue))
                                    {
                                        break;
                                    }
                                    
                                }
                            }
                            else
                            {
                               break;
                            }
                        }
                        
                        
                        
                        for(int b = 0; b < maxColumnNumberRow; b++)
                        {
                            dataFormatter = new DataFormatter();
                            cellValue = dataFormatter.formatCellValue(row.getCell(b));
                            switch(b)
                            {
                                case 0:                                 
                                if(StringUtils.isBlank(cellValue))
                                {
                                    sale.setFecha("");
                                    sale.setFecha2("");
                                }
                                else
                                {
                                    dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                    date = row.getCell(b).getDateCellValue(); 
                                    sale.setFecha(dateFormat.format(date));
                                    dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                    sale.setFecha2(dateFormat.format(date));
                                }
                                break;
                                case 1: if(StringUtils.isBlank(cellValue)){sale.setPedidoAdicional("");}else{sale.setPedidoAdicional(cellValue);} break;
                                case 2: if(StringUtils.isBlank(cellValue)){sale.setFactura("");}else{sale.setFactura(cellValue);} break;
                                case 3: if(StringUtils.isBlank(cellValue)){sale.setFolio("");}else{sale.setFolio(cellValue);} break;
                                case 4: if(StringUtils.isBlank(cellValue)){sale.setSolicitante("");}else{sale.setSolicitante(cellValue);} break;
                                case 5: if(StringUtils.isBlank(cellValue)){sale.setCedis("");}else{sale.setCedis(cellValue);} break;
                                case 6: if(StringUtils.isBlank(cellValue)){sale.setDestinatario("");}else{sale.setDestinatario(cellValue);} break;
                                case 7:
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        sale.setNombreDestinatario("");
                                        sale.setNombreDestinatario2("");
                                    }
                                    else
                                    {
                                        sale.setNombreDestinatario(cellValue); 
                                        nombreDestinatario2 = ""; 
                                        nombreDestinatario2 = cellValue.substring(cellValue.indexOf("-")+1, cellValue.length());
                                        nombreDestinatario2 = nombreDestinatario2.replace(" ", "");
                                        nombreDestinatario2 = StringUtils.stripAccents(nombreDestinatario2); 
                                        nombreDestinatario2 = nombreDestinatario2.replaceAll("[^a-zA-Z0-9]+", "");
                                        nombreDestinatario2 = nombreDestinatario2.toLowerCase();
                                        sale.setNombreDestinatario2(nombreDestinatario2);
                                    }
                                break;
                                case 8: if(StringUtils.isBlank(cellValue)){sale.setFacturaRemisionSicav("");}else{sale.setFacturaRemisionSicav(cellValue);} break; 
                                case 9: if(StringUtils.isBlank(cellValue)){ sale.setImporte(""); } else { sale.setImporte(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 10: if(StringUtils.isBlank(cellValue)){sale.setCliente("");}else{sale.setCliente(cellValue);} break;
                                case 11: if(StringUtils.isBlank(cellValue)){sale.setRefFact("");}else{sale.setRefFact(cellValue);} break;
                                case 12: if(StringUtils.isBlank(cellValue)){sale.setReferencia("");}else{sale.setReferencia(cellValue);}  break;
                                case 13: if(StringUtils.isBlank(cellValue)){sale.setClvRef2("");}else{sale.setClvRef2(cellValue);} break;
                                case 14: if(StringUtils.isBlank(cellValue)){sale.setClvRef3("");}else{sale.setClvRef3(cellValue);} break;
                                case 15: 
                                    if(StringUtils.isBlank(cellValue))
                                    { 
                                        sale.setFechaDoc("");
                                        sale.setFechaDoc2("");
                                    } 
                                    else 
                                    { 
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        date = row.getCell(b).getDateCellValue();
                                        sale.setFechaDoc(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        sale.setFechaDoc2(dateFormat.format(date));
                                    }
                                break;
                                case 16: 
                                    if(StringUtils.isBlank(cellValue))
                                    {
                                        sale.setVencNeto("");
                                        sale.setVencNeto2("");
                                    }
                                    else
                                    {
                                        dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                                        date = row.getCell(b).getDateCellValue();
                                        sale.setVencNeto(dateFormat.format(date));
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        sale.setVencNeto2(dateFormat.format(date));
                                    } 
                                    break;
                                case 17: if(StringUtils.isBlank(cellValue)) { sale.setImpteMl(""); } else { sale.setImpteMl(decimalFormat.format(row.getCell(b).getNumericCellValue())); } break;
                                case 18: if(StringUtils.isBlank(cellValue)){sale.setCe("");}else{sale.setCe(cellValue);} break;
                                case 19: if(StringUtils.isBlank(cellValue)){sale.setDiv("");}else{sale.setDiv(cellValue);} break;
                                default: break;
                            }
                        }
                        listSale.add(sale); 
                    }
                    else if(maxColumnNumberRow == 20 && checkSaleColumns == false)
                    {
                        checkSaleColumns = true;
                    }
                }
            }
            databaseUtilities.insertIntoSale(server, databasePath, TABLE_SALES, salesColumns, listSale);
            //databaseUtilities.insertIntoVirtualSale(server, databasePath, TABLE_VIRTUAL_SALES, virtualSalesColumns, listSale);
            server.info("Insert Data Information into ventas Table was sucessful");
            server.info("Close Excel File");
            book.close();
            
        }
        
        public void processFirstJoin() throws Exception
        {
            //int number1 = 0; 
            //int number2 = 0;
            int idFolio = 0; 
            FileInputStream inputFile = null;
            Workbook book;
            Sheet linkSheet;
            Row rowColumn = null; 
            ArrayList<Link> listLink;
            Document document = new Document(); 
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            Iterator<Row> iterator1 = null;
            Iterator<Row> iterator2 = null;
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Get Sheet from Excel File");
            linkSheet = book.createSheet("Puntos 8-9");
            String[] columnNames = new String[]{"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", " ", "Pedido Adicional", "CR Tienda", "Num de Remisión", "Fecha", "Neto", " ", "Diferencia", "%", " ", "Destinatario", " Tipo de Busqueda", "Tipo de Busqueda"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = linkSheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + linkSheet.getSheetName());
            listFirstJoin = databaseUtilities.joinByRemissionAndAdditionalOrder(server, databasePath, idFolio);
            server.info("Total de Link Elementos: " + listFirstJoin.size());
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Datos del Punto 8 y 9");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
            
            for(Link link: listFirstJoin)
            {
                rowCount++;
                Row row = linkSheet.createRow(rowCount);
                columnCount = 0;
                listRemoveIdSaleNotMatch.add(link.getVenta().getId());
                listRemoveIdReceptionNotMatch.add(link.getRecepcion().getId());
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
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel");
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
            ArrayList<Link> listLink;
            Document document = new Document(); 
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            Iterator<Row> iterator1 = null;
            Iterator<Row> iterator2 = null;
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
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
            listTenPoint = databaseUtilities.searchByStoreDateAmount(server, databasePath, idFolio, listFirstJoin);
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
                listRemoveIdSaleNotMatch.add(link.getVenta().getId());
                listRemoveIdReceptionNotMatch.add(link.getRecepcion().getId());
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
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel");
        }
        
        public void processNotMatch() throws Exception
        {
            //int number1 = 0; 
            //int number2 = 0;
            int idFolio = 0; 
            ArrayList<Sale> listSale = new ArrayList<Sale>();
            ArrayList<Reception> listReception = new ArrayList<Reception>();
            FileInputStream inputFile = null;
            Workbook book;
            Sheet sheet;
            Row rowColumn = null; 
            Document document = new Document(); 
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Create Sheet from Excel File");
            sheet = book.createSheet("Ventas Lala Punto 11");
            String[] columnNames = new String[]{"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", "Cliente", "Ref. fact.", "Referencia", "Clv. ref.2", "Clv. ref.3", "Fecha doc.", "Venc.neto", "ImpteML", "Ce", "Div"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = sheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + sheet.getSheetName());
            listSale = databaseUtilities.getSalesNotMatch(server, databasePath, idFolio, listFirstJoin);
            //listReception = databaseUtilities.getReceptionsNotMatch(server, databasePath, idFolio, listFirstJoin);
            server.info("Total de Ventas que no tienen una relacion: " + listSale.size());
            server.info("Total de Ventas reportadas por Tienda que no tienen relacion: " + listReception.size()); 
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Ventas que no tienen relacion para el Punto 11");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
            
            for(Sale sale: listSale)
            {
                rowCount++;
                Row row = sheet.createRow(rowCount);
                columnCount = 0;
                listIdSaleNotMatch.add(sale.getId());
                for(String columnNameValue: columnNames)
                {
                    Cell cell = row.createCell(columnCount);
                    switch(columnCount)
                    {
                        case 0: if(StringUtils.isBlank(sale.getFecha())){cell.setCellValue("");}else{cell.setCellValue(sale.getFecha());} columnCount++; break; 
                        case 1: if(StringUtils.isBlank(sale.getPedidoAdicional())){cell.setCellValue("");}else{cell.setCellValue(sale.getPedidoAdicional());} columnCount++; break;
                        case 2: if(StringUtils.isBlank(sale.getFactura())){cell.setCellValue("");}else{cell.setCellValue(sale.getFactura());} columnCount++; break;
                        case 3: if(StringUtils.isBlank(sale.getFolio())){cell.setCellValue("");}else{cell.setCellValue(sale.getFolio());} columnCount++; break;
                        case 4: if(StringUtils.isBlank(sale.getSolicitante())){cell.setCellValue("");}else{cell.setCellValue(sale.getSolicitante());} columnCount++; break;
                        case 5: if(StringUtils.isBlank(sale.getCedis())){cell.setCellValue("");}else{cell.setCellValue(sale.getCedis());} columnCount++; break;
                        case 6: if(StringUtils.isBlank(sale.getDestinatario())){cell.setCellValue("");}else{cell.setCellValue(sale.getDestinatario());} columnCount++; break;
                        case 7: if(StringUtils.isBlank(sale.getNombreDestinatario())){cell.setCellValue("");}else{cell.setCellValue(sale.getNombreDestinatario());} columnCount++; break;
                        case 8: if(StringUtils.isBlank(sale.getFacturaRemisionSicav())){cell.setCellValue("");}else{cell.setCellValue(sale.getFacturaRemisionSicav().substring(0, 3));} columnCount++; break;
                        case 9: if(StringUtils.isBlank(sale.getFacturaRemisionSicav())){cell.setCellValue("");}else{cell.setCellValue(sale.getFacturaRemisionSicav().substring(3, sale.getFacturaRemisionSicav().length()));} columnCount++; break;
                        case 10: if(StringUtils.isBlank(sale.getImporte())){cell.setCellValue("");}else{cell.setCellValue(sale.getImporte());} columnCount++; break;
                        case 11: if(StringUtils.isBlank(sale.getCliente())){cell.setCellValue("");}else{cell.setCellValue(sale.getCliente());} columnCount++; break;
                        case 12: if(StringUtils.isBlank(sale.getRefFact())){cell.setCellValue("");}else{cell.setCellValue(sale.getRefFact());} columnCount++; break;
                        case 13: if(StringUtils.isBlank(sale.getReferencia())){cell.setCellValue("");}else{cell.setCellValue(sale.getReferencia());} columnCount++; break;
                        case 14: if(StringUtils.isBlank(sale.getClvRef2())){cell.setCellValue("");}else{cell.setCellValue(sale.getClvRef2());} columnCount++; break;
                        case 15: if(StringUtils.isBlank(sale.getClvRef3())){cell.setCellValue("");}else{cell.setCellValue(sale.getClvRef3());} columnCount++; break;
                        case 16: if(StringUtils.isBlank(sale.getFechaDoc())){cell.setCellValue("");}else{cell.setCellValue(sale.getFechaDoc());} columnCount++; break;
                        case 17: if(StringUtils.isBlank(sale.getVencNeto())){cell.setCellValue("");}else{cell.setCellValue(sale.getVencNeto());} columnCount++; break;
                        case 18: if(StringUtils.isBlank(sale.getImpteMl())){cell.setCellValue("");}else{cell.setCellValue(sale.getImpteMl());} columnCount++; break;
                        case 19: if(StringUtils.isBlank(sale.getCe())){cell.setCellValue("");}else{cell.setCellValue(sale.getCe());} columnCount++; break;
                        case 20: if(StringUtils.isBlank(sale.getDiv())){cell.setCellValue("");}else{cell.setCellValue(sale.getDiv());} columnCount++; break;
                        
                        default: break; 
                    }
                }
            }
            
            /*for(Reception reception: listReception)
            {
                rowCount++;
                Row row = sheet.createRow(rowCount);
                columnCount = 0;
                for(String columnNameValue: columnNames)
                {
                    Cell cell = row.createCell(columnCount);
                    switch(columnCount)
                    {
                        case 0: columnCount++; break; 
                        case 1: columnCount++; break;
                        case 2: columnCount++; break;
                        case 3: columnCount++; break;
                        case 4: columnCount++; break;
                        case 5: columnCount++; break;
                        case 6: columnCount++; break;
                        case 7: columnCount++; break;
                        case 8: columnCount++; break;
                        case 9: columnCount++; break;
                        case 10: columnCount++; break;
                        case 11: cell.setCellValue(""); columnCount++; break;
                        case 12: if(StringUtils.isBlank(reception.getAdicional())){cell.setCellValue("");}else{cell.setCellValue(reception.getAdicional());} columnCount++; break;
                        case 13: if(StringUtils.isBlank(reception.getTienda())){cell.setCellValue("");}else{cell.setCellValue(reception.getTienda());} columnCount++; break;
                        case 14: if(StringUtils.isBlank(reception.getRemision())){cell.setCellValue("");}else{cell.setCellValue(reception.getRemision());} columnCount++; break;
                        case 15: if(StringUtils.isBlank(reception.getFecha())){cell.setCellValue("");}else{cell.setCellValue(reception.getFecha());} columnCount++; break;
                        case 16: if(StringUtils.isBlank(reception.getNeto())){cell.setCellValue("");}else{cell.setCellValue(reception.getNeto());} columnCount++; break;
                        default: break; 
                    }
                }
            }*/
            server.info("Añadiendolos al archivo Excel");
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel"); 
        }
        
        public void processReceptionNotMatch() throws Exception
        {
            //int number1 = 0; 
            //int number2 = 0;
            int idFolio = 0; 
            ArrayList<Sale> listSale = new ArrayList<Sale>();
            ArrayList<Reception> listReception = new ArrayList<Reception>();
            FileInputStream inputFile = null;
            Workbook book;
            Sheet sheet;
            Row rowColumn = null; 
            Document document = new Document(); 
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Create Sheet from Excel File");
            sheet = book.createSheet("Ventas Oxxo Punto 11");
            String[] columnNames = new String[]{"MVTO", "Tienda", "Recibo", "Orden", "Adicional", "Remision", "Fecha", "Valor", "Iva", "Neto"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = sheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + sheet.getSheetName());
            listReception = databaseUtilities.getReceptionsNotMatch(server, databasePath, idFolio, listFirstJoin);
            server.info("Total de Ventas reportadas por Tienda que no tienen relacion: " + listReception.size()); 
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Ventas que no tienen relacion para el Punto 11");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
            
            for(Reception reception: listReception)
            {
                rowCount++;
                Row row = sheet.createRow(rowCount);
                columnCount = 0;
                listIdReceptionNotMatch.add(reception.getId());
                for(String columnNameValue: columnNames)
                {
                    Cell cell = row.createCell(columnCount);
                    switch(columnCount)
                    {
                        case 0: if(StringUtils.isBlank(reception.getMtvo())){cell.setCellValue("");}else{cell.setCellValue(reception.getMtvo());} columnCount++; break; 
                        case 1: if(StringUtils.isBlank(reception.getTienda())){cell.setCellValue("");}else{cell.setCellValue(reception.getTienda());} columnCount++; break;
                        case 2: if(StringUtils.isBlank(reception.getRecibo())){cell.setCellValue("");}else{cell.setCellValue(reception.getRecibo());} columnCount++; break;
                        case 3: if(StringUtils.isBlank(reception.getOrden())){cell.setCellValue("");}else{cell.setCellValue(reception.getOrden());} columnCount++; break;
                        case 4: if(StringUtils.isBlank(reception.getAdicional())){cell.setCellValue("");}else{cell.setCellValue(reception.getAdicional());} columnCount++; break;
                        case 5: if(StringUtils.isBlank(reception.getRemision())){cell.setCellValue("");}else{cell.setCellValue(reception.getRemision());} columnCount++; break;
                        case 6: if(StringUtils.isBlank(reception.getFecha())){cell.setCellValue("");}else{cell.setCellValue(reception.getFecha());} columnCount++; break;
                        case 7: if(StringUtils.isBlank(reception.getValor())){cell.setCellValue("");}else{cell.setCellValue(reception.getValor());} columnCount++;; break;
                        case 8: if(StringUtils.isBlank(reception.getIva())){cell.setCellValue("");}else{cell.setCellValue(reception.getIva());} columnCount++;; break;
                        case 9: if(StringUtils.isBlank(reception.getNeto())){cell.setCellValue("");}else{cell.setCellValue(reception.getNeto());} columnCount++; break;
                        default: break; 
                    }
                }
            }
            server.info("Añadiendolos al archivo Excel");
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel"); 
        }
        
        public void processPointTwelve() throws Exception
        {
            int currentIdFolio = 0; 
            int pastIdFolio = 0; 
            ArrayList<Link> listLink = new ArrayList<Link>();
            FileInputStream inputFile = null;
            Workbook book;
            Sheet sheet;
            Row rowColumn = null; 
            Document document = new Document(); 
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            server.info("Obtain Current Id From documento Table");
            currentIdFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Obtain Past Id From documento Table");
            document = new Document();
            document.setNumeroFolio(pastFolioNumber);
            document.setNombre(pastFileName);
            pastIdFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document); 
            listLink = databaseUtilities.searchLastWeek(server, databasePath, currentIdFolio, pastIdFolio);
            server.info("Numero de Registros para el Punto 12: " + listLink.size());
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Create Sheet from Excel File");
            sheet = book.createSheet("Punto 12");
            String[] columnNames = new String[]{"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", " ", "Pedido Adicional", "CR Tienda", "Num de Remisión", "Fecha", "Neto", " ", "Diferencia", "%", " ", "Destinatario", " Tipo de Busqueda", "Tipo de Busqueda"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = sheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + sheet.getSheetName());
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Ventas que tienen relacion con un reporte de la semana pasada para el Punto 12");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
           
            for(Link link: listLink)
            {
                rowCount++;
                Row row = sheet.createRow(rowCount);
                columnCount = 0;
                listRemoveIdSaleNotMatch.add(link.getVenta().getId());
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
                        case 23: cell.setCellValue(pastFileName); columnCount++; break;
                        default: break; 
                    }
                }
            }
            server.info("Añadiendolos al archivo Excel");
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel");
        }
        
        public void processLaterPointTwelve() throws Exception
        {
            int currentIdFolio = 0; 
            int pastIdFolio = 0; 
            ArrayList<Link> listLink = new ArrayList<Link>();
            FileInputStream inputFile = null;
            Workbook book;
            Sheet sheet;
            Row rowColumn = null; 
            Document document = new Document(); 
            document.setNumeroFolio(laterFolioNumber);
            document.setNombre(laterFileName);
            server.info("Obtain Later Id From documento Table");
            currentIdFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Obtain Current Id From documento Table");
            document = new Document();
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            pastIdFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document); 
            listLink = databaseUtilities.searchLastWeek(server, databasePath, currentIdFolio, pastIdFolio);
            server.info("Numero de Registros que hacen relacion con un archivo Posterior: " + listLink.size());
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Create Sheet from Excel File");
            sheet = book.createSheet("Ventas Oxxo Relacion Posterior");
            String[] columnNames = new String[]{"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", " ", "Pedido Adicional", "CR Tienda", "Num de Remisión", "Fecha", "Neto", " ", "Diferencia", "%", " ", "Destinatario", " Tipo de Busqueda", "Tipo de Busqueda"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = sheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + sheet.getSheetName());
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Ventas que tienen relacion con un reporte de la semana posterior");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
           
            for(Link link: listLink)
            {
                rowCount++;
                Row row = sheet.createRow(rowCount);
                columnCount = 0;
                listRemoveIdReceptionNotMatch.add(link.getRecepcion().getId());
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
                        case 23: cell.setCellValue(laterFileName); columnCount++; break;
                        default: break; 
                    }
                }
            }
            server.info("Añadiendolos al archivo Excel");
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel");
        }
        
        public void getRecepctionByCutDate() throws Exception
        {
            int idFolio = 0;
            Document document = null;
            ArrayList<Integer> listIdReception = null;
            ArrayList<Integer> listIdSale = null;
            ArrayList<Link> listLink = new ArrayList<Link>();
            
            HashSet<Integer> listIdRecepctionDelete = new HashSet<Integer>(listRemoveIdReceptionNotMatch);
            HashSet<Integer> listIdSaleDelete = new HashSet<Integer>(listRemoveIdSaleNotMatch);
            
            listIdReception = new ArrayList<Integer>(listIdRecepctionDelete);
            listIdSale = new ArrayList<Integer>(listIdSaleDelete);
            FileInputStream inputFile = null;
            Workbook book;
            Sheet sheet;
            Row rowColumn = null;
            server.info("Total Id Reception Not Match que quedan por procesar antes de eliminar: " + listIdReceptionNotMatch.size()); 
            server.info("Total de Id a eliminar que no se repiten: " + listIdReception.size()); 
            if(listIdReceptionNotMatch != null && listRemoveIdReceptionNotMatch != null && listIdReception != null)
            {
                for(int a = 0; a < listIdReception.size(); a++)
                {
                    /*if(listIdReceptionNotMatch.contains(listIdReception.get(a)) == false)
                    {
                        server.info("Esta recepcion no se encuentra en la lista pero en la relacion si: " + listIdReception.get(a));
                    }*/
                    if(listIdReceptionNotMatch.contains(listIdReception.get(a)) == true)
                    {
                        listIdReceptionNotMatch.remove(listIdReception.get(a));
                    }
                }
            }
            server.info("Total Id Reception Not Match que quedan por procesar: " + listIdReceptionNotMatch.size()); 
            
            if(listIdSaleNotMatch != null && listRemoveIdSaleNotMatch != null && listIdSale != null)
            {
                server.info("Total de la lista a eliminar: " + listRemoveIdSaleNotMatch.size());
                server.info("Total de la lista que no tiene relacion LALA: " + listIdSaleNotMatch.size());
                server.info("Total de la lista a eliminar que no se repite: " + listIdSale.size());
                for(int b = 0; b < listIdSale.size(); b++)
                {
                    /*if(listIdSaleNotMatch.contains(listIdSale.get(b)) == false)
                    {
                        server.info("Esta venta no se encuentra en la lista pero en la relacion si: " +  listIdSale.get(b)); 
                    }*/
                    
                    listIdSaleNotMatch.remove(listIdSale.get(b));
                }
            }
            server.info("Total Id Ventas Not Match que quedan por procesar: " + listIdSaleNotMatch.size());
            
            document = new Document();
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            
            listLink = databaseUtilities.getMatchSaleByCutDate(server, databasePath, idFolio, listIdReceptionNotMatch, listIdSaleNotMatch);
            
            server.info("Numero de Registros que hacen relacion por fecha de Corte: " + listLink.size());
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Create Sheet from Excel File");
            sheet = book.createSheet("Ventas Oxxo Fecha Corte");
            String[] columnNames = new String[]{"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", " ", "Pedido Adicional", "CR Tienda", "Num de Remisión", "Fecha", "Neto", " ", "Diferencia", "%", " ", "Destinatario", " Tipo de Busqueda", "Tipo de Busqueda"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = sheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + sheet.getSheetName());
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Ventas que tienen relacion por Fecha de Corte");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
           
            for(Link link: listLink)
            {
                rowCount++;
                Row row = sheet.createRow(rowCount);
                columnCount = 0;
                listRemoveIdSaleNotMatch.add(link.getVenta().getId());
                listRemoveIdReceptionNotMatch.add(link.getRecepcion().getId());
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
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel");
        }
        
        public void getReceptionFinalNotMatch() throws Exception
        {
            int idFolio = 0;
            Document document = null;
            ArrayList<Integer> listIdReception = null;
            ArrayList<Integer> listIdSale = null;
            ArrayList<Reception> listReception = new ArrayList<Reception>();
            ArrayList<Link> listLink = new ArrayList<Link>();
            
            HashSet<Integer> listIdRecepctionDelete = new HashSet<Integer>(listRemoveIdReceptionNotMatch);
            HashSet<Integer> listIdReceptionNoRelation = new HashSet<Integer>(listIdReceptionNotMatch);
            HashSet<Integer> listIdSaleDelete = new HashSet<Integer>(listRemoveIdSaleNotMatch);
            HashSet<Integer> listIdSaleNotRelation = new HashSet<Integer>(listIdSaleNotMatch);
            
            listIdReception = new ArrayList<Integer>(listIdRecepctionDelete);
            listIdSale = new ArrayList<Integer>(listIdSaleDelete);
            FileInputStream inputFile = null;
            Workbook book;
            Sheet sheet;
            Row rowColumn = null;
            if(listIdReceptionNotMatch != null && listRemoveIdReceptionNotMatch != null && listIdReception != null)
            {
                for(int a = 0; a < listIdReception.size(); a++)
                {
                    /*if(listIdReceptionNotMatch.contains(listIdReception.get(a)) == false)
                    {
                        server.info("Esta recepcion no se encuentra en la lista pero en la relacion si: " + listIdReception.get(a));
                    }*/
                    
                    listIdReceptionNotMatch.remove(listIdReception.get(a));
                }
            }
            server.info("Total Id Reception Not Match que quedan por procesar: " + listIdReceptionNotMatch.size()); 
            
            if(listIdSaleNotMatch != null && listRemoveIdSaleNotMatch != null && listIdSale != null)
            {
                for(int b = 0; b < listIdSale.size(); b++)
                {
                    /*if(listIdSaleNotMatch.contains(listIdSale.get(b)) == false)
                    {
                        server.info("Esta venta no se encuentra en la lista pero en la relacion si: " +  listIdSale.get(b)); 
                    }*/
                    
                    listIdSaleNotMatch.remove(listIdSale.get(b));
                }
            }
            server.info("Total Id Ventas Not Match que quedan por procesar: " + listIdSaleNotMatch.size());
            
            document = new Document();
            document.setNumeroFolio(folioNumber);
            document.setNombre(currentfileName);
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            
            listReception = databaseUtilities.getFinalReceptionsNotMatch(server, databasePath, listIdReceptionNotMatch);
            
            server.info("Total de Registros que ya no cuentan con relacion: " + listReception.size()); 
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Create Sheet from Excel File");
            sheet = book.createSheet("Ventas Oxxo Sin Relacion");
            String[] columnNames = new String[]{"MVTO", "Tienda", "Recibo", "Orden", "Adicional", "Remision", "Fecha", "Valor", "Iva", "Neto"};
            int rowCount = 0; 
            int columnCount = 0;
            rowColumn = sheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + sheet.getSheetName());
            server.info("Escribiendo Ventas que tienen relacion con un reporte de la semana posterior");
            for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }
            
            for(Reception reception: listReception)
            {
                rowCount++;
                Row row = sheet.createRow(rowCount);
                columnCount = 0;
                for(String columnNameValue: columnNames)
                {
                    Cell cell = row.createCell(columnCount);
                    switch(columnCount)
                    {
                        case 0: if(StringUtils.isBlank(reception.getMtvo())){cell.setCellValue("");}else{cell.setCellValue(reception.getMtvo());} columnCount++; break; 
                        case 1: if(StringUtils.isBlank(reception.getTienda())){cell.setCellValue("");}else{cell.setCellValue(reception.getTienda());} columnCount++; break;
                        case 2: if(StringUtils.isBlank(reception.getRecibo())){cell.setCellValue("");}else{cell.setCellValue(reception.getRecibo());} columnCount++; break;
                        case 3: if(StringUtils.isBlank(reception.getOrden())){cell.setCellValue("");}else{cell.setCellValue(reception.getOrden());} columnCount++; break;
                        case 4: if(StringUtils.isBlank(reception.getAdicional())){cell.setCellValue("");}else{cell.setCellValue(reception.getAdicional());} columnCount++; break;
                        case 5: if(StringUtils.isBlank(reception.getRemision())){cell.setCellValue("");}else{cell.setCellValue(reception.getRemision());} columnCount++; break;
                        case 6: if(StringUtils.isBlank(reception.getFecha())){cell.setCellValue("");}else{cell.setCellValue(reception.getFecha());} columnCount++; break;
                        case 7: if(StringUtils.isBlank(reception.getValor())){cell.setCellValue("");}else{cell.setCellValue(reception.getValor());} columnCount++;; break;
                        case 8: if(StringUtils.isBlank(reception.getIva())){cell.setCellValue("");}else{cell.setCellValue(reception.getIva());} columnCount++;; break;
                        case 9: if(StringUtils.isBlank(reception.getNeto())){cell.setCellValue("");}else{cell.setCellValue(reception.getNeto());} columnCount++; break;
                        default: break; 
                    }
                }
            }
            server.info("Añadiendolos al archivo Excel");
            FileOutputStream outputStream = new FileOutputStream(currentExcelFilePath);
            book.write(outputStream);
            book.close();
            outputStream.close();
            server.info("Cerrado Excel");
        }
        
        public void processLinkIntoExcel() throws Exception
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
            document.setNombre(currentfileName);
            Iterator<Row> iterator1 = null;
            Iterator<Row> iterator2 = null;
            server.info("Obtain Id From documento Table");
            idFolio = databaseUtilities.getIdFolio(server, databasePath, TABLE_DOCUMENTS, document);
            server.info("Open Excel File"); 
            inputFile = new FileInputStream(new File(currentExcelFilePath));
            
            server.info("Obtain Workbook from Excel File");
            book = WorkbookFactory.create(inputFile);
            //book = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(inputFile);
            server.info("Get Sheet from Excel File");
            linkSheet = book.getSheetAt(4);
            String[] columnNames = new String[]{"Fecha", "Pedido Adicional", "Factura", "Folio", "Solicitante", "Cedis", "Destinatario", "Nombre del Destinatario", "Factura", "Remisión Sicav", "Importe", " ", "Pedido Adicional", "CR Tienda", "Num de Remisión", "Fecha", "Neto", " ", "Diferencia", "%", " ", "Destinatario", " Tipo de Busqueda", "Tipo de Busqueda"};
            int rowCount = 0; 
            int columnCount = 0;
            int timesBlank = 0;
            //rowColumn = linkSheet.createRow(rowCount);
            server.info("Read All Filled Row From Sheet: " + linkSheet.getSheetName());
            listTenPoint = databaseUtilities.searchByStoreDateAmount(server, databasePath, idFolio, listFirstJoin);
            server.info("Total de Link Elementos: " + listTenPoint.size());
            //server.info("Total de Filas: " + linkSheet.getLastRowNum());
            server.info("Escribiendo Datos del Punto 10");
            /*for(String columnName: columnNames)
            {
                Cell cellName = rowColumn.createCell(columnCount);
                cellName.setCellValue(columnName);
                columnCount++;
            }*/
            server.info("Total de Filas: " + linkSheet.getLastRowNum());
            for(int a = 0; a < linkSheet.getLastRowNum(); a++)
            {
                Row currentSheetRow = linkSheet.getRow(a);
                
                if(currentSheetRow != null)
                {
                    timesBlank = 0; 
                    Iterator<Cell> iteratorCell = currentSheetRow.iterator();
                    columnCount = 0;
                    while(iteratorCell.hasNext())
                    {
                        Cell cell = iteratorCell.next(); 
                        if(cell.getCellTypeEnum() == CellType.BLANK || cell.getCellTypeEnum() == CellType._NONE)
                        {
                            columnCount++;
                        }
                        else 
                        {
                            timesBlank = 0;
                        }
                        
                        if(columnCount == 24)
                        {
                            timesBlank++;
                        }
                    }
                }
                else if(currentSheetRow == null)
                {
                    timesBlank++;
                }
                
                if(timesBlank == 10)
                {
                    server.info("Indice en donde esta vacio: " + a);
                    break;
                }
                
                /*for(Link link: listTenPoint)
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
                }*/
            }
            server.info("Añadiendolos al archivo Excel");
            //FileOutputStream outputStream = new FileOutputStream(excelFilePath);
            //book.write(outputStream);
            book.close();
            //outputStream.close();
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
	
        public String hasLaterFile() throws Exception {
            String response = "no";
            if(existLaterInputFile == true)
            {
                response = "yes";
            }
            else if(existLaterInputFile == false)
            {
                response = "no";
            }
            return response;
	}
        
        public String hastPastFile() throws Exception {
            String response = "no";
            if(existPastInputFile == true)
            {
                response = "yes"; 
            }
            else if(existPastInputFile == false) 
            {
                response = "no"; 
            }
            
            return response;
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
		return new String[] { currentExcelFilePath };
	}
}
