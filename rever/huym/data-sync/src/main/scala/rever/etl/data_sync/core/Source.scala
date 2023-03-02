package rever.etl.data_sync.core

import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 05/07/2022
  */
trait Source[T] {
  val config: Config

  /**
   *
   * @param consumeFn take 3 params: <br>
   *                  + The 1st: record <br>
   *                  + 2nd: Total records of the current page <br>
   *                  + 3rd: Total records to be synced <br>
   *
   * @return
   */
  def pull(consumeFn: (T, Long, Long) => Unit): Long

}
