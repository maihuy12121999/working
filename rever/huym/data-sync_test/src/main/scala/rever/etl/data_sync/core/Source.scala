package rever.etl.data_sync.core

import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 05/07/2022
  */
trait Source[T] {
  val config: Config

  def pull(consumeFn: (T, Long, Long) => Unit): Long

}
