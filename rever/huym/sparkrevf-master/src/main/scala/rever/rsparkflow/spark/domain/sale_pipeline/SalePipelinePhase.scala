package rever.rsparkflow.spark.domain.sale_pipeline

/**
  * @author anhlt (andy)
  * @since 30/07/2022
**/
case class SalePipelinePhase(
    id: Int,
    pipelineId: Int,
    name: String,
    description: Option[String],
    position: Int,
    createdTime: Option[Long],
    updatedTime: Option[Long],
    status: Int,
    createdBy: Option[String],
    updatedBy: Option[String]
)
