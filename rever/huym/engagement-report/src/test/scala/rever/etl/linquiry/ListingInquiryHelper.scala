package rever.etl.engagement.linquiry

import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import rever.etl.engagement.linquiry.config.ListingInquiryFields

object ListingInquiryHelper {
  final val inquirySchema =
    StructType(Array(
      StructField(ListingInquiryFields.INQUIRY_ID,StringType,true),
      StructField(ListingInquiryFields.CREATED_TIME,LongType,true),
      StructField(ListingInquiryFields.UPDATED_TIME,LongType,true),
      StructField(ListingInquiryFields.OWNER_ID,StringType,true),
      StructField(ListingInquiryFields.OWNER_TEAM,StringType,true),
      StructField(ListingInquiryFields.OWNER_EMAIL,StringType,true),
      StructField(ListingInquiryFields.OWNER_JOB_TITLE,StringType,true),
      StructField(ListingInquiryFields.OWNER_DEPARTMENT,StringType,true),
      StructField(ListingInquiryFields.LISTING_IDS,StringType,true),
  ))

  final val listingSchema =
    StructType(Array(
      StructField(ListingInquiryFields.LISTING_ID,StringType,true),
      StructField(ListingInquiryFields.PUBLISHED_TIME,LongType,true),
      StructField(ListingInquiryFields.UPDATED_TIME,LongType,true),
      StructField(ListingInquiryFields.LISTING_STATUS,StringType,true),
      StructField(ListingInquiryFields.PROPERTY_TYPE,StringType,true),
      StructField(ListingInquiryFields.NUM_BED_ROOM,IntegerType,true),
      StructField(ListingInquiryFields.SALE_PRICE,DoubleType,true),
      StructField(ListingInquiryFields.AREA_USING,DoubleType,true),
      StructField(ListingInquiryFields.CITY,StringType,true),
      StructField(ListingInquiryFields.DISTRICT,StringType,true),
      StructField(ListingInquiryFields.WARD,StringType,true),
      StructField(ListingInquiryFields.PROJECT_NAME,StringType,true),
      StructField(ListingInquiryFields.OWNER_ID,StringType,true),
      StructField(ListingInquiryFields.OWNER_TEAM,StringType,true),
      StructField(ListingInquiryFields.OWNER_EMAIL,StringType,true),
      StructField(ListingInquiryFields.OWNER_JOB_TITLE,StringType,true),
      StructField(ListingInquiryFields.OWNER_DEPARTMENT,StringType,true)
  ))

  final val listingInquirySchema =
    StructType(Array(
      StructField(ListingInquiryFields.LISTING_ID,StringType,true),
      StructField(ListingInquiryFields.INQUIRY_ID,StringType,true),
      StructField(ListingInquiryFields.CREATED_TIME,LongType,true),
      StructField(ListingInquiryFields.UPDATED_TIME_INQUIRY,LongType,true),
      StructField(ListingInquiryFields.OWNER_ID,StringType,true),
      StructField(ListingInquiryFields.OWNER_TEAM,StringType,true),
      StructField(ListingInquiryFields.OWNER_EMAIL,StringType,true),
      StructField(ListingInquiryFields.OWNER_JOB_TITLE,StringType,true),
      StructField(ListingInquiryFields.OWNER_DEPARTMENT,StringType,true),
      StructField(ListingInquiryFields.PUBLISHED_TIME,LongType,true),
      StructField(ListingInquiryFields.UPDATED_TIME_LISTING,LongType,true),
      StructField(ListingInquiryFields.LISTING_STATUS,StringType,true),
      StructField(ListingInquiryFields.PROPERTY_TYPE,StringType,true),
      StructField(ListingInquiryFields.NUM_BED_ROOM,IntegerType,true),
      StructField(ListingInquiryFields.SALE_PRICE,DoubleType,true),
      StructField(ListingInquiryFields.AREA_USING,DoubleType,true),
      StructField(ListingInquiryFields.CITY,StringType,true),
      StructField(ListingInquiryFields.DISTRICT,StringType,true),
      StructField(ListingInquiryFields.WARD,StringType,true),
      StructField(ListingInquiryFields.PROJECT_NAME,StringType,true)
    ))
}
