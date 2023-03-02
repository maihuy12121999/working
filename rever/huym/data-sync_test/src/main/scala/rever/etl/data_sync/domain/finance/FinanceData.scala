package rever.etl.data_sync.domain.finance

case class FinanceData(
    brokerages: Seq[BrokerageRecord],
    nonBrokerages: Seq[NonBrokerageRecord],
    backOffices: Seq[PlatformRecord]
)
