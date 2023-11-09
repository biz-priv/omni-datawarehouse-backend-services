const AWS = require("aws-sdk");
const { Converter } = AWS.DynamoDB;
const dynamodb = new AWS.DynamoDB.DocumentClient();
const ddb = new AWS.DynamoDB.DocumentClient();
const { get } = require("lodash");
const moment = require('moment');
const { getDynamodbData } = require("../shared/common_functions/shipment_details");

//const { refParty, pieces, actualWeight, ChargableWeight, weightUOM, getTime, locationFunc, getShipmentDate, getPickupTime, upsertItem, MappingDataToInsert, getDynamodbData } = require("../../shared/commonFunctions/shipment_details");
//const { tableValues,weightDimensionValue,INDEX_VALUES,customerTypeValue, } = require("../../shared/constants/shipment_details");

module.exports.handler = async (event) => {
  console.log("event: ", JSON.stringify(event));
  
  const unmarshalledData = Converter.unmarshall(
    event.Records[0].dynamodb.NewImage
  );
  let orderNo = unmarshalledData.PK_OrderNo;
  console.log("orderNo",orderNo)
  await getDynamodbData(orderNo);
};
