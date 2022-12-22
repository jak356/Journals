package com.bitpanda;

import com.netsuite.webservices.platform.core_2021_2.Record;
import com.netsuite.webservices.platform.core_2021_2.RecordRef;
import com.netsuite.webservices.platform.messages_2021_2.UpsertListRequest;
import com.netsuite.webservices.transactions.employees_2021_2.ExpenseReportExpense;
import com.netsuite.webservices.transactions.general_2021_2.JournalEntry;
import com.netsuite.webservices.transactions.general_2021_2.JournalEntryLine;
import com.netsuite.webservices.transactions.general_2021_2.JournalEntryLineList;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import javax.management.RuntimeErrorException;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JournalsRecordHandler extends RecordHandler{
    private static String appName;
    private static final String QUERY = "SELECT TC.externalId,\n" +
            "TC.subsidiaryId,\n" +
            "TC.memo,\n" +
            "TC.toSubsidiaryId\n" +
            "C.key,\n" +
            "TC.exchangeRate,\n" +
            "TC.tranDate,\n" +
            "PP.key \n"+
            "TC.lines \n"+
            "FROM "+ appName +"_table_changes TC \n" +
            "LEFT JOIN bp_db.currency C ON TC.currency = C.value \n" +
            "LEFT JOIN bp_db.posting_period PP ON TC.postingPeriod = PP.value \n" +
            "WHERE \"date\"= Cast(current_date as varchar)";

    private final Map<String,String> accountMap;

    private final Map<String,String> locationMap;



    JournalsRecordHandler(String date, String appName) {
        super(date);
        JournalsRecordHandler.appName = appName;
        try {
            accountMap = getS3Records("bitpanda-finops-staging-constant-data-cdc","constants/account/account.csv");
            locationMap = getS3Records("bitpanda-finops-staging-constant-data-cdc","constants/location/location.csv");

        } catch (IOException e) {
            logger.error("Error loading csv constant data");
            throw new RuntimeException(e);
        }

    }
    @Override
    void processRow(List<Row> row) throws DatatypeConfigurationException, JAXBException, UnsupportedEncodingException {
        logger.info("Entering processRow");

        int total = row.size();
        logger.info("Total elements: {}", total);

        for (int i = 0; i < ((total / 100) + 1); i++) {
            logger.info("blucle for -> i:{}", i);
            UpsertListRequest upsertListRequest = new UpsertListRequest();
            List<Record> records = upsertListRequest.getRecord();
            logger.info("Pre-bucle");
            for (int j = 1, index = i == 0 ? j : (i * 100) + j;
                 j <= 100 && index < total;
                 j++, index++) {
                logger.info("recorriendo rows");
                Row myRow = row.get(index);
                List<Datum> datum = myRow.data();
                JournalEntry journalEntry = new JournalEntry();
                RecordRef postingPeriod = new RecordRef();
                RecordRef currency = new RecordRef();
                RecordRef subsidiary = new RecordRef();
                RecordRef toSubsidiary = new RecordRef();
                Double exchangeRate = Double.parseDouble(datum.get(3).varCharValue());
                String externalId = datum.get(29).varCharValue();
                String memo = datum.get(19).varCharValue();

                try {
                    postingPeriod.setInternalId(datum.get(0).varCharValue());
                    currency.setInternalId(datum.get(2).varCharValue());
                    subsidiary.setInternalId(datum.get(16).varCharValue());
                    toSubsidiary.setInternalId(datum.get(20).varCharValue());

                } catch (NullPointerException nullPointerException) {
                    logger.info(String.valueOf(nullPointerException));
                }

                journalEntry.setPostingPeriod(postingPeriod);
                XMLGregorianCalendar tranDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(datum.get(1).varCharValue());
                journalEntry.setTranDate(tranDate);
                journalEntry.setCurrency(currency);
                journalEntry.setSubsidiary(subsidiary);
                journalEntry.setToSubsidiary(toSubsidiary);
                journalEntry.setExchangeRate(exchangeRate);
                journalEntry.setExternalId(externalId);
                journalEntry.setMemo(memo);


                JournalEntryLineList journalEntryLineList = new JournalEntryLineList();
                String lines = datum.get(4).varCharValue();
                try{
                    JSONParser parser = new JSONParser();
                    Object obj = parser.parse(lines);
                    JSONArray array = (JSONArray) obj;

                    logger.info("array size={}",array.toArray().length);

                    for(int k = 0;k<array.toArray().length; k++ ) {
                        JSONObject obj2 = (JSONObject) array.get(k);
                        JournalEntryLine journalEntryLine = new JournalEntryLine();
                        RecordRef account = new RecordRef();
                        RecordRef clazz = new RecordRef();
                        RecordRef department = new RecordRef();
                        RecordRef location = new RecordRef();
                        Double credit = Double.valueOf(obj2.get("Line_Credit").toString());
                        Double debit = Double.valueOf(obj2.get("Line_Debit").toString());
                        String memo1 = obj2.get("Line_Memo").toString();
                        RecordRef entity = new RecordRef();
                        Boolean eliminate = Boolean.valueOf(obj2.get("Line_Eliminate").toString());
                        RecordRef taxCode = new RecordRef();
                        Double tax1Amt = Double.valueOf(obj2.get("Line_Tax1_Amt").toString());

                        try{
                            account.setInternalId(accountMap.get(obj2.get("Line_Account").toString()));
                            clazz.setInternalId(obj2.get("Line_Class").toString());
                            department.setInternalId(obj2.get("Line_Department").toString());
                            location.setInternalId(locationMap.get(obj2.get("Line_Location").toString()));
                            entity.setInternalId(obj2.get("Line_Entity").toString());
                            taxCode.setInternalId(obj2.get("Line_Tax_Code").toString());
                        } catch (NullPointerException nullPointerException) {
                            logger.info(String.valueOf(nullPointerException));
                        }
                        journalEntryLine.setAccount(account);
                        journalEntryLine.setClazz(clazz);
                        journalEntryLine.setDepartment(department);
                        journalEntryLine.setLocation(location);
                        journalEntryLine.setCredit(credit);
                        journalEntryLine.setDebit(debit);
                        journalEntryLine.setMemo(memo1);
                        journalEntryLine.setEntity(entity);
                        journalEntryLine.setEliminate(eliminate);
                        journalEntryLine.setTaxCode(taxCode);
                        journalEntryLine.setTax1Amt(tax1Amt);

                        journalEntryLineList.getLine().add(journalEntryLine);


                    }
                    } catch (ParseException pe){
                        logger.info(String.valueOf(pe));
                    }
                journalEntry.setLineList(journalEntryLineList);
                records.add(journalEntry);
            }
            buildAndSaveXML(upsertListRequest, i,"xml_journals/" + appName + "/date=" + getDate() +"/" + appName);
        }

    }
    public Map<String, String> getS3Records(String bucket, String key) throws IOException {
        logger.info("Entering getS3Records with bucket:{} and key:{}", bucket, key);
        Map<String, String> records = new HashMap<>();
        try (CSVReader reader = getReader(bucket, key)) {
            for (String[] keyValuePar : reader) {
                String mapKey = keyValuePar[1];
                String mapValue = keyValuePar[0];
                logger.info("Record key:{}, value:{}", mapKey, mapValue);
                records.put(mapKey, mapValue);
            }

            return records;
        }
    }
    private CSVReader getReader(String bucket, String key) {
        logger.info("Entering CSVReader");
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(';')
                .build();
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        ResponseBytes<GetObjectResponse> object = getS3().getObjectAsBytes(objectRequest);
        InputStreamReader inputStreamReader = new InputStreamReader(object.asInputStream());

        return new CSVReaderBuilder(inputStreamReader)
                .withCSVParser(parser)
                .build();
    }

    private S3Client getS3() {
        logger.info("Entering getS3");
        return S3Client.builder()
                .region(Region.EU_WEST_1)
                .build();
    }

    @Override
    String getQuery() {
        return QUERY;
    }
}

