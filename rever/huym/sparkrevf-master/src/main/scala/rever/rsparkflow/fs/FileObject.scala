package rever.rsparkflow.fs

import software.amazon.awssdk.services.s3.model.S3Object

import java.io.File

trait FileObject[T] {
  val data: T
}

case class S3File(data: S3Object) extends FileObject[S3Object]
case class LocalFile(data: File) extends FileObject[File]
