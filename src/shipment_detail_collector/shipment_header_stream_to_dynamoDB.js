const AWS = require("aws-sdk");
const { Converter } = AWS.DynamoDB;
const dynamodb = new AWS.DynamoDB.DocumentClient();
const ddb = new AWS.DynamoDB.DocumentClient();
const { get } = require("lodash");
const moment = require("moment");
const {
  getDynamodbData,
} = require("../shared/common_functions/shipment_details");

module.exports.handler = async (event) => {
  console.info("event: ", JSON.stringify(event));
  for (let record of event.Records) {
    const unmarshalledData = Converter.unmarshall(record.dynamodb.NewImage);
    let orderNo = unmarshalledData.PK_OrderNo;
    console.info("orderNo", orderNo);
    if (orderNo !== "" && orderNo !== null) {
      await getDynamodbData(orderNo);
    }
  }
  return "Success";
};
