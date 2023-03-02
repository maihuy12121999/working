package rever.etl.data_sync.client

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.sheets.v4.model._
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes

import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

/** @author anhlt (andy)
  */

object GSheetClient {

  private val clientMap = scala.collection.mutable.Map.empty[String, GSheetClient]

  def client(serviceAccountContent: String): GSheetClient = {
    val cacheKey = serviceAccountContent
    clientMap.get(cacheKey) match {
      case Some(client) => client
      case None =>
        clientMap.synchronized {
          clientMap
            .get(cacheKey)
            .fold[GSheetClient]({
              val credentials: GoogleCredentials = GoogleCredentials
                .fromStream(new ByteArrayInputStream(serviceAccountContent.getBytes("UTF-8")))
                .createScoped(SheetsScopes.all())
              val client = GSheetClient(credentials)
              clientMap.put(cacheKey, client)
              client
            })(x => x)

        }
    }
  }

  def toRowData(row: Row): RowData = {
    new RowData().setValues(
      row.schema.fields.zipWithIndex
        .map { case (f, i) =>
          new CellData()
            .setUserEnteredValue(
              f.dataType match {
                case DataTypes.StringType    => new ExtendedValue().setStringValue(row.getString(i))
                case DataTypes.LongType      => new ExtendedValue().setNumberValue(row.getLong(i).toDouble)
                case DataTypes.IntegerType   => new ExtendedValue().setNumberValue(row.getInt(i).toDouble)
                case DataTypes.FloatType     => new ExtendedValue().setNumberValue(row.getFloat(i).toDouble)
                case DataTypes.BooleanType   => new ExtendedValue().setBoolValue(row.getBoolean(i))
                case DataTypes.DateType      => new ExtendedValue().setStringValue(row.getDate(i).toString)
                case DataTypes.ShortType     => new ExtendedValue().setNumberValue(row.getShort(i).toDouble)
                case DataTypes.TimestampType => new ExtendedValue().setStringValue(row.getTimestamp(i).toString)
                case DataTypes.DoubleType    => new ExtendedValue().setNumberValue(row.getDouble(i))
              }
            )
        }
        .toList
        .asJava
    )
  }

  final val SHEET_RANGE_PATTERN = "(?<fromRange>[\\s&!\\w]+):(?<toRange>\\w+)".r

  def parseRange(rangePattern: String): (String, String) = {
    val SHEET_RANGE_PATTERN(fromRange, toRange) = rangePattern

    (fromRange, toRange)
  }

  def buildActualRange(range: String, fromIdx: Int, toIdx: Int): String = {
    val SHEET_RANGE_PATTERN(fromRange, toRange) = range

    s"${fromRange}${fromIdx}:${toRange}${toIdx}"
  }
}

case class GSheetClient(credentials: GoogleCredentials) {

  private final lazy val client = new Sheets.Builder(
    GoogleNetHttpTransport.newTrustedTransport(),
    com.google.api.client.json.jackson2.JacksonFactory.getDefaultInstance,
    new HttpCredentialsAdapter(credentials)
  ).setApplicationName("support-report").build()

  val counter = new AtomicInteger()

  def findSheetId(spreadsheetId: String, sheetName: String): Int = {

    val sheetIdOpt = client
      .spreadsheets()
      .get(spreadsheetId)
      .execute()
      .getSheets
      .asScala
      .find(_.getProperties.getTitle == sheetName)
      .map(_.getProperties.getSheetId)

    sheetIdOpt.getOrElse(throw new Exception(s"No sheet was found with name: ${sheetName}"))
  }

  def setSheetMaxRows(
      spreadsheetId: String,
      sheetId: Int,
      sheetName: String,
      columnCount: Int,
      maxRowCount: Int
  ): Unit = {

    val batchUpdateSpreadsheetRequest = new BatchUpdateSpreadsheetRequest()

    batchUpdateSpreadsheetRequest.setRequests(
      Seq(
        new Request().setUpdateSheetProperties(
          new UpdateSheetPropertiesRequest()
            .setFields("*")
            .setProperties(
              new SheetProperties()
                .setSheetId(sheetId)
                .setTitle(sheetName)
                .setGridProperties(
                  new GridProperties()
                    .setRowCount(maxRowCount)
                    .setColumnCount(columnCount)
                )
            )
        )
      ).asJava
    )

    client
      .spreadsheets()
      .batchUpdate(spreadsheetId, batchUpdateSpreadsheetRequest)
      .execute()

  }

  def clearSheetRange(
      spreadsheetId: String,
      sheetId: Int,
      startColumnIndex: Int,
      endColumnIndex: Int,
      startRowIndex: Int,
      endRowIndex: Int
  ): Unit = {

    val batchUpdateSpreadsheetRequest = new BatchUpdateSpreadsheetRequest()

    batchUpdateSpreadsheetRequest.setRequests(
      Seq(
        new Request().setDeleteRange(
          new DeleteRangeRequest()
            .setRange(
              new GridRange()
                .setSheetId(sheetId)
                .setStartRowIndex(startRowIndex)
                .setEndRowIndex(endRowIndex)
                .setStartColumnIndex(startColumnIndex)
                .setEndColumnIndex(endColumnIndex)
            )
            .setShiftDimension("ROWS")
        )
      ).asJava
    )

    client
      .spreadsheets()
      .batchUpdate(spreadsheetId, batchUpdateSpreadsheetRequest)
      .execute()

  }

  def beginWriteAt(index: Int): Unit = {
    counter.addAndGet(index)
  }

  def appendRows(spreadsheetId: String, sheetId: Int, rows: Seq[Row]): Unit = {

    val batchUpdateSpreadsheetRequest = new BatchUpdateSpreadsheetRequest()

    val dataRows = rows.map(GSheetClient.toRowData).asJava

    val updateRowsRequest = new UpdateCellsRequest()
      .setStart(
        new GridCoordinate()
          .setSheetId(sheetId)
          .setRowIndex(counter.get())
          .setColumnIndex(0)
      )
      .setRows(dataRows)
      .setFields("userEnteredValue")

    batchUpdateSpreadsheetRequest.setRequests(
      Seq(new Request().setUpdateCells(updateRowsRequest)).asJava
    )

    val responses = client
      .spreadsheets()
      .batchUpdate(spreadsheetId, batchUpdateSpreadsheetRequest)
      .execute()

    counter.addAndGet(dataRows.size())

  }

  def getSheetValue(
      id: String,
      range: String,
      isRow: Boolean = true,
      renderOption: String = "FORMATTED_VALUE"
  ): ValueRange = {
    client
      .spreadsheets()
      .values()
      .get(id, range)
      .setMajorDimension(if (isRow) "ROWS" else "COLUMNS")
      .setValueRenderOption(renderOption)
      .setDateTimeRenderOption("FORMATTED_STRING")
      .execute()

  }

  def readRows(
      id: String,
      rangePattern: String,
      from: Int,
      to: Int,
      renderOption: String = "FORMATTED_VALUE"
  ): Seq[Seq[Any]] = {
    val v = client
      .spreadsheets()
      .values()
      .get(id, GSheetClient.buildActualRange(rangePattern, from, to))
      .setValueRenderOption(renderOption)
      .setDateTimeRenderOption("FORMATTED_STRING")
      .execute()
      .getValues

    if (v != null) {
      v.asScala
        .map(_.asScala.toSeq)
        .filterNot(isEmptyRow)
        .filterNot(_.isEmpty)
        .toSeq
    } else {
      Seq.empty
    }
  }

  def foreachRows(
      id: String,
      fromColumn: String,
      beginRow: Int = 1,
      batchSize: Int = 100,
      renderOption: String = "UNFORMATTED_VALUE"
  )(fn: Seq[Any] => Unit): Unit = {
    var rowIdx: Int = beginRow
    var lastRows: Seq[Seq[Any]] = Seq.empty
    try {
      do {
        lastRows = readRows(id, fromColumn, rowIdx, rowIdx + batchSize, renderOption)
        lastRows.filterNot(isEmptyRow).foreach(fn(_))
        rowIdx += batchSize
      } while (lastRows.nonEmpty && !lastRows.forall(isEmptyRow))
    } catch {
      case e: GoogleJsonResponseException if e.getMessage.contains("exceeds grid limits") =>
      case e: Exception                                                                   => throw e
    }
  }

  private def isEmptyRow(row: Seq[Any]): Boolean = {
    !row.filterNot(_ == null).exists(_.toString.trim.nonEmpty)
  }

}
