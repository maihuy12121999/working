package rever.etl.data_sync.jobs.finance

trait BaseGSheetReader[T] {

  def read(spreadsheetId: String, sheet: String): Seq[T]
}
