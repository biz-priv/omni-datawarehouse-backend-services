const AWS = require("aws-sdk");
const { Converter } = AWS.DynamoDB;
const { get } = require("lodash");
const sns = new AWS.SNS();
const {getDynamodbData,} = require("../shared/common_functions/shipment_details");
const {
  CONSIGNEE_TABLE,
  REFERENCE_TABLE,
  SHIPMENT_DESC_TABLE,
  SHIPMENT_HEADER_TABLE,
  SHIPMENT_MILESTONE_DETAIL_TABLE,
  SHIPMENT_MILESTONE_TABLE,
  SHIPPER_TABLE,
  TRACKING_NOTES_TABLE,
  CUSTOMER_ENTITLEMENT_TABLE
} = process.env;

module.exports.handler = async (event, context) => {
  console.info("event: ", JSON.stringify(event));
  for (let record of event.Records) {
    const tableArn = record["eventSourceARN"];
    const tableName = get(tableArn.split(":table/")[1].split("/"), '[0]', '');
    console.log("tableName", tableName);
    const unmarshalledData = Converter.unmarshall(record.dynamodb.NewImage);
    let orderNo;
    if ( tableName === SHIPMENT_DESC_TABLE || tableName === SHIPMENT_MILESTONE_TABLE || tableName === TRACKING_NOTES_TABLE ) {
      orderNo = get(unmarshalledData, 'FK_OrderNo', '');
    } else if ( tableName === SHIPMENT_MILESTONE_DETAIL_TABLE && get(unmarshalledData, 'FK_OrderStatusId', '') === 'PUP' ) {
      orderNo = get(unmarshalledData, 'FK_OrderNo', '');
    } else if (tableName === CONSIGNEE_TABLE) {
      orderNo = get(unmarshalledData, 'FK_ConOrderNo', '');
    } else if (tableName === REFERENCE_TABLE) {
      orderNo = get(unmarshalledData, 'FK_OrderNo', '');
    } else if (tableName === SHIPMENT_HEADER_TABLE) {
      orderNo = get(unmarshalledData, 'PK_OrderNo', '');
    } else if (tableName === SHIPPER_TABLE) {
      orderNo = get(unmarshalledData, 'FK_ShipOrderNo', '');
    }else if(tableName === CUSTOMER_ENTITLEMENT_TABLE){
      orderNo = get(unmarshalledData, 'FileNumber', '');
    }
    console.info("orderNo", orderNo);
    if (orderNo) {
      try{
        await getDynamodbData(orderNo);
      }
      catch(error){
        console.error("error in getDynamodbData function: ", error)
        try {
          const params = {
            Message: `An error occurred in function ${context.functionName}.\n\nERROR DETAILS: ${error}.\n\norderNo: ${orderNo}`,
            Subject: `An error occured in function ${context.functionName}`,
            TopicArn: process.env.ERROR_SNS_TOPIC_ARN,
          };
          await sns.publish(params).promise();
          console.info('SNS notification has sent');
        } catch (err) {
          console.error('Error while sending sns notification: ', err);
        }
      }
    }
  }
  return "Success";
};
