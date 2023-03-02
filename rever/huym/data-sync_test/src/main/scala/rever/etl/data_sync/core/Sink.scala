package rever.etl.data_sync.core

import rever.etl.rsparkflow.api.configuration.Config

trait Sink[T] {

  val config: Config

  def writeBatch(records: Seq[T]): Int

  def write(record: T): Int

  def end(): Unit
}
