package google_sheet_reader

import com.google.api.client.googleapis.json.{GoogleJsonError, GoogleJsonResponseException}
import com.google.api.client.http.HttpRequestInitializer
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sheets.v4.SheetsScopes
import com.google.api.services.sheets.v4.model.{AppendValuesResponse, BatchGetValuesResponse, BatchUpdateValuesRequest, BatchUpdateValuesResponse, UpdateValuesResponse, ValueRange}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials

import java.util._
import java.util.Collections
import com.google.api.services.sheets.v4.Sheets

import java.util

object GoogleSheetInteraction {
  def main(args: Array[String]): Unit = {
//    ====================================read================================
//    single range
    val spreadsheetId = "1bFiBG11gCHbO_ay8xwISODWAO8yygRL79DX21ubFCsI"
    val result = getValues(spreadsheetId,"people_data!A1:C4")
    println(result.getValues)
//    multiple ranges
    var ranges = util.Arrays.asList("people_data!A1:C4","people_data!A10:C13")
    println(batchGetValues(spreadsheetId,ranges))
//    ==================================write=================================
//    single range
    val valueInputOption = "USER_ENTERED"
    var values :util.List[util.List[Object]] = util.Arrays.asList(
      util.Arrays.asList("4","tuan","23"),
      util.Arrays.asList("5","hieu","23"),
      util.Arrays.asList("6","long","23"),
    )
    println(updateValues(spreadsheetId,"people_data!A5:C7",valueInputOption,values))
//    multiple ranges
    ranges = util.Arrays.asList("people_data!A5:C7","people_data!A14:C16")
    val multipleValues:util.List[util.List[util.List[Object]]] =
      util.Arrays.asList(
        util.Arrays.asList(
          util.Arrays.asList("4","tuan","23"),
          util.Arrays.asList("5","hieu","23"),
          util.Arrays.asList("6","long","23"),
        ),
        util.Arrays.asList(
          util.Arrays.asList("4","tuan","23"),
          util.Arrays.asList("5","hieu","23"),
          util.Arrays.asList("6","long","23"),
        ),
      )
    println(batchUpdateValues(spreadsheetId,ranges,valueInputOption,multipleValues))
//    ====================================append========================================
    values= util.Arrays.asList(
      util.Arrays.asList("7","zed","23"),
      util.Arrays.asList("8","shen","23"),
    )
    println(appendValues(spreadsheetId,"people_data!A1",valueInputOption,values))
  }
  def batchGetValues(spreadsheetId:String,ranges:util.List[String]):BatchGetValuesResponse={
    val credentials = GoogleCredentials.getApplicationDefault().createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS))
    val requestInitializer:HttpRequestInitializer = new HttpCredentialsAdapter(credentials)
    val service = new Sheets
    .Builder(
      new NetHttpTransport(),
      GsonFactory.getDefaultInstance,
      requestInitializer).setApplicationName("Google sheet reader").build()
    var result:BatchGetValuesResponse = null
    try{
      result=service.spreadsheets().values().batchGet(spreadsheetId).setRanges(ranges).execute()
      println(s"ranges retrieved: ${result.getValueRanges.size()}")
    }
    catch{
      case e:GoogleJsonResponseException=>
        val error:GoogleJsonError = e.getDetails
        if(error.getCode == 404){
          println(s"Spreadsheet not found with id: $spreadsheetId")
        }
        else{
          throw e
        }
    }
    result
  }
  def getValues(spreadsheetId:String,range:String):ValueRange={
    val credentials = GoogleCredentials.getApplicationDefault().createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS))
    val requestInitializer:HttpRequestInitializer = new HttpCredentialsAdapter(credentials)
    val service = new Sheets
    .Builder(
      new NetHttpTransport(),
      GsonFactory.getDefaultInstance,
      requestInitializer).setApplicationName("Google sheet reader").build()
    var result:ValueRange = null
    try{
      result=service.spreadsheets().values().get(spreadsheetId,range).execute()
      val numRows:Int = result.getValues match {
        case null => 0
        case _ => result.getValues.size()
      }
      println(s"row retrieved: $numRows")
    }
    catch{
      case e:GoogleJsonResponseException=>
        val error:GoogleJsonError = e.getDetails
        if(error.getCode == 404){
          println(s"Spreadsheet not found with id: $spreadsheetId")
        }
        else{
          throw e
        }
    }
    result
  }
  def updateValues(spreadsheetId:String,range: String, valueInputOption:String,values: util.List[util.List[Object]]):UpdateValuesResponse={
    val credentials = GoogleCredentials.getApplicationDefault().createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS))
    val requestInitializer:HttpRequestInitializer = new HttpCredentialsAdapter(credentials)
    val service = new Sheets
    .Builder(
      new NetHttpTransport(),
      GsonFactory.getDefaultInstance,
      requestInitializer).setApplicationName("Google sheet reader").build()
    var result:UpdateValuesResponse = null
    try{
      val body = new ValueRange().setValues(values)
      result=service.spreadsheets()
        .values()
        .update(spreadsheetId,range,body)
        .setValueInputOption(valueInputOption)
        .execute()
      println(s"cell updated: ${result.getUpdatedCells}")
    }
    catch{
      case e:GoogleJsonResponseException=>
        val error:GoogleJsonError = e.getDetails
        if(error.getCode == 404){
          println(s"Spreadsheet not found with id: $spreadsheetId")
        }
        else{
          throw e
        }
    }
    result
  }
  def batchUpdateValues(spreadsheetId:String, ranges: util.List[String], valueInputOption:String, values:util.List[util.List[util.List[Object]]]):BatchUpdateValuesResponse={
    val credentials = GoogleCredentials.getApplicationDefault().createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS))
    val requestInitializer:HttpRequestInitializer = new HttpCredentialsAdapter(credentials)
    val service = new Sheets
    .Builder(
      new NetHttpTransport(),
      GsonFactory.getDefaultInstance,
      requestInitializer).setApplicationName("Google sheet reader").build()
    val data = new util.ArrayList[ValueRange]()
    var id =0
    if(ranges.size()==values.size()) {
      ranges.forEach(
        range => {
          data.add(new ValueRange().setRange(range).setValues(values.get(id)))
          id += 1
        }
      )
    }
    var result : BatchUpdateValuesResponse = null
    try{
      val body = new BatchUpdateValuesRequest()
        .setValueInputOption(valueInputOption).setData(data)
      result=service.spreadsheets()
        .values()
        .batchUpdate(spreadsheetId,body)
        .execute()
      println(s"cells updated: ${result.getTotalUpdatedCells}")
    }
    catch{
      case e:GoogleJsonResponseException=>
        val error:GoogleJsonError = e.getDetails
        if(error.getCode == 404){
          println(s"Spreadsheet not found with id: $spreadsheetId")
        }
        else{
          throw e
        }
    }
    result
  }
  def appendValues(spreadsheetId:String,range: String, valueInputOption:String,values: util.List[util.List[Object]]):AppendValuesResponse={
    val credentials = GoogleCredentials.getApplicationDefault().createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS))
    val requestInitializer:HttpRequestInitializer = new HttpCredentialsAdapter(credentials)
    val service = new Sheets
    .Builder(
      new NetHttpTransport(),
      GsonFactory.getDefaultInstance,
      requestInitializer).setApplicationName("Google sheet reader").build()
    var result:AppendValuesResponse = null
    try{
      val body = new ValueRange().setValues(values)
      result=service.spreadsheets()
        .values()
        .append(spreadsheetId,range,body)
        .setValueInputOption(valueInputOption)
        .execute()
      println(s"cells appended: ${result.getUpdates.getUpdatedCells}")
    }
    catch{
      case e:GoogleJsonResponseException=>
        val error:GoogleJsonError = e.getDetails
        if(error.getCode == 404){
          println(s"Spreadsheet not found with id: $spreadsheetId")
        }
        else{
          throw e
        }
    }
    result

  }
}
