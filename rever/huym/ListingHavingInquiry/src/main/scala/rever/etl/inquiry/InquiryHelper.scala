package rever.etl.inquiry

import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import rever.etl.inquiry.config.InquiryFields

object InquiryHelper {
  final val inquirySchema =
    StructType(Array(
      StructField(InquiryFields.INQUIRY_ID,StringType,true),
      StructField(InquiryFields.CREATED_TIME,LongType,true),
      StructField(InquiryFields.UPDATED_TIME,LongType,true),
      StructField(InquiryFields.OWNER_ID,StringType,true),
      StructField(InquiryFields.OWNER_TEAM,StringType,true),
      StructField(InquiryFields.LISTING_IDS,StringType,true),
  ))
  final val listingInquirySchema =
    StructType(Array(
      StructField(InquiryFields.LISTING_ID,StringType,true),
      StructField(InquiryFields.INQUIRY_ID,StringType,true),
      StructField(InquiryFields.CREATED_TIME,LongType,true),
      StructField(InquiryFields.UPDATED_TIME_INQUIRY,LongType,true),
      StructField(InquiryFields.OWNER_ID,StringType,true),
      StructField(InquiryFields.OWNER_TEAM,StringType,true),
      StructField(InquiryFields.OWNER_EMAIL,StringType,true),
      StructField(InquiryFields.OWNER_JOB_TITLE,StringType,true),
      StructField(InquiryFields.OWNER_DEPARTMENT,StringType,true),
      StructField(InquiryFields.PUBLISHED_TIME,LongType,true),
      StructField(InquiryFields.UPDATED_TIME_LISTING,LongType,true),
      StructField(InquiryFields.LISTING_STATUS,StringType,true),
      StructField(InquiryFields.PROPERTY_TYPE,StringType,true),
      StructField(InquiryFields.NUM_BED_ROOM,IntegerType,true),
      StructField(InquiryFields.SALE_PRICE,FloatType,true),
      StructField(InquiryFields.AREA_USING,FloatType,true),
      StructField(InquiryFields.CITY,StringType,true),
      StructField(InquiryFields.DISTRICT,StringType,true),
      StructField(InquiryFields.WARD,StringType,true),
      StructField(InquiryFields.PROJECT_NAME,StringType,true)
    ))
}
