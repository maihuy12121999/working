package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.mutable.ListBuffer

trait BoTeamSizeReader extends BaseGSheetReader[BoTeamSize] {
  def read(spreadsheetId: String, sheet: String): Seq[BoTeamSize]
}

case class BoTeamSizeReaderImpl(client: GSheetClient, dataRange: String, mappings: Map[String, Int])
    extends BoTeamSizeReader {

  override def read(spreadsheetId: String, sheet: String): Seq[BoTeamSize] = {
    val records = ListBuffer.empty[BoTeamSize]

    client.foreachRows(spreadsheetId, s"${sheet}!${dataRange}", 3, 10) { row =>
      val dataRow = row.map(_.asInstanceOf[AnyRef])

      records.appendAll(toRecords(dataRow))
    }

    testData(records)
    records
  }

  private def toRecords(sheetRowValues: Seq[AnyRef]): Seq[BoTeamSize] = {

    val monthInTime: Long = (sheetRowValues lift mappings("time"))
      .map(_.toString)
      .map(TimestampUtils.parseMillsFromString(_, "MMM-yy"))
      .get

    val team = (sheetRowValues lift mappings("team")).map(_.toString).getOrElse("")
    val teamSize = (sheetRowValues lift mappings("team_size")).map(FinanceGSheetUtils.asDouble).getOrElse(0.0).toInt

    Seq(
      BoTeamSize(
        periodType = Some("MONTH"),
        periodValue = Some(TimestampUtils.asMonth(monthInTime)),
        team = Some(team),
        teamSize = Some(teamSize),
        time = Some(monthInTime),
        logTime = Some(System.currentTimeMillis())
      )
    )

  }

  /** This method is used to validate data is correct
    */
  private def testData(records: Seq[BoTeamSize]): Unit = {

    val pRecord = records
      .filter(_.team.exists(_.equals("Customer Excellence")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head

    assertResult(pRecord.teamSize.getOrElse(0), "Wrong headcount")(23)

  }

}
