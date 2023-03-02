package rever.etl.call.jobs.non_call_users

import com.amazonaws.services.ec2.model.Storage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel
import rever.etl.call.domain.{CallServiceNames, NonCallAgentFields}
import rever.etl.call.jobs.non_call_users.reader.{CallAgentReader, CsvCallAgentReader, UserHistoricalReader}
import rever.etl.call.jobs.non_call_users.writer.NonCallAgentWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.IntervalTypes

class NonCallAgentReport extends FlowMixin{
  @Output(writer = classOf[NonCallAgentWriter])
  @Table("call_non_using_agent_df")
  def build(
            @Table(
              name = "call_history",
              reader = classOf[CsvCallAgentReader]
            ) dailyUserUsingCallDf:DataFrame,
            @Table(
              name = "curent_a0_users",
              reader = classOf[UserHistoricalReader]
            ) currentA0UserDf:DataFrame,
            config: Config
  ):DataFrame={
    dailyUserUsingCallDf.persist(StorageLevel.MEMORY_AND_DISK_2)
    val servicePhonedA0UserDf = getUserHavingServicePhone(currentA0UserDf).persist(StorageLevel.MEMORY_AND_DISK_2)
    val dailyUserNonUsingCallDf =
      getDailUserNonUsingCallDf(dailyUserUsingCallDf,servicePhonedA0UserDf,config)
        .withColumn(NonCallAgentFields.CALL_SERVICE,lit(CallServiceNames.ALL))

    val idb3cxCallAgentDf =
      dailyUserUsingCallDf
        .where(col(NonCallAgentFields.CALL_SERVICE)===CallServiceNames.IDB_3CX)
    val idb3cxNonCallAgentDf =
      getDailUserNonUsingCallDf(idb3cxCallAgentDf,servicePhonedA0UserDf,config)
        .withColumn(NonCallAgentFields.CALL_SERVICE,lit(CallServiceNames.IDB_3CX))

    val stringeeCallAgentDf =
      dailyUserUsingCallDf
        .where(col(NonCallAgentFields.CALL_SERVICE)===CallServiceNames.STRINGEE)
    val stringeeNonCallAgentDf =
      getDailUserNonUsingCallDf(stringeeCallAgentDf,servicePhonedA0UserDf,config)
        .withColumn(NonCallAgentFields.CALL_SERVICE,lit(CallServiceNames.STRINGEE))

    val vhtcallCallAgentDf =
      dailyUserUsingCallDf
        .where(col(NonCallAgentFields.CALL_SERVICE)===CallServiceNames.VHTCALL)
    val vhtcallNonCallAgentDf =
      getDailUserNonUsingCallDf(vhtcallCallAgentDf,servicePhonedA0UserDf,config)
        .withColumn(NonCallAgentFields.CALL_SERVICE,lit(CallServiceNames.VHTCALL))

    val vhtcall2CallAgentDf =
      dailyUserUsingCallDf
        .where(col(NonCallAgentFields.CALL_SERVICE)===CallServiceNames.VHTCALL_2)
    val vhtcall2NonCallAgentDf =
      getDailUserNonUsingCallDf(vhtcall2CallAgentDf,servicePhonedA0UserDf,config)
        .withColumn(NonCallAgentFields.CALL_SERVICE,lit(CallServiceNames.VHTCALL_2))

    dailyUserNonUsingCallDf
      .unionByName(idb3cxNonCallAgentDf)
      .unionByName(stringeeNonCallAgentDf)
      .unionByName(vhtcallNonCallAgentDf)
      .unionByName(vhtcall2NonCallAgentDf)
  }
  private def getUserHavingServicePhone(currentA0UserDf:DataFrame):DataFrame={
    currentA0UserDf
  }
  private def getDailUserNonUsingCallDf(dailyUserUsingCallDf:DataFrame,servicePhonedA0UserDf:DataFrame,config:Config):DataFrame={
    val reportTime = config.getDailyReportTime
    val df = servicePhonedA0UserDf
      .join(dailyUserUsingCallDf,
        servicePhonedA0UserDf(NonCallAgentFields.USERNAME)=== dailyUserUsingCallDf(NonCallAgentFields.AGENT_ID),
      "leftanti")
      .select(
        lit(IntervalTypes.DAILY).as(NonCallAgentFields.DATA_TYPE),
        lit(config.getDailyReportTime).as(NonCallAgentFields.DATE),
        servicePhonedA0UserDf(NonCallAgentFields.USERNAME),
        servicePhonedA0UserDf(NonCallAgentFields.TEAM_ID),
        servicePhonedA0UserDf(NonCallAgentFields.MARKET_CENTER_ID)
      )
    df
      .na
      .replace(
        Seq(
          NonCallAgentFields.TEAM_ID,
          NonCallAgentFields.MARKET_CENTER_ID
        ),
        Map(""->"unknown")
      )
      .na
      .fill("unknown")
  }
}
