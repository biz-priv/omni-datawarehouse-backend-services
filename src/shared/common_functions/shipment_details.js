const AWS = require("aws-sdk");
const moment = require('moment');
const { get } = require('lodash');
const momentTZ = require("moment-timezone");
const ddb = new AWS.DynamoDB.DocumentClient();

const { tableValues, weightDimensionValue, INDEX_VALUES, customerTypeValue, statusCodes } = require("../constants/shipment_details");

async function refParty(customerType) {
  try {
    let refParty = get(customerTypeValue, `${customerType}`, null);
    if (refParty == null) {
      refParty = get(customerTypeValue, "default", null);
    }
    return refParty;
  } catch (error) {
    throw error;
  }
}
async function pieces(tableValue) {
  try {
    let pieces = 0;
    await Promise.all(tableValue.map(async (val) => {
      pieces += Number(val.Pieces);
    }));
    return Number(pieces);
  } catch (error) {
    throw error;
  }
}
async function actualWeight(tableValue) {
  try {
    let actualWeight = 0;
    await Promise.all(tableValue.map(async (val) => {
      actualWeight += Number(val.Weight);
    }));
    return Number(actualWeight);
  } catch (error) {
    throw error;
  }
}
async function ChargableWeight(tableValue) {
  try {
    let weightSum = 0;
    let dimSum = 0;

    await Promise.all(tableValue.map(async (val) => {
      const weight = Number(val.Weight);
      const pieces = Number(val.Pieces);
      const length = Number(val.Length);
      const width = Number(val.Width);
      const height = Number(val.Height);
      const dimfactor = Number(val.DimFactor);

      weightSum += weight;

      const dimWeight = Math.ceil((pieces * length * width * height) / dimfactor);
      dimSum += dimWeight;
    }));

    return Math.max(weightSum, dimSum);
  } catch (error) {
    throw error;
  }
}
async function weightUOM(weightDimension) {
  try {
    let weightUOM = get(weightDimensionValue, `${weightDimension}`, null);
    if (weightUOM == null) {
      weightUOM = get(weightDimensionValue, "default", null);
    }
    return weightUOM;
  } catch (error) {
    throw error;
  }
}

async function getPickupTime(dateTime, dateTimeZone, timeZoneTable) {
  try {
    const result = await getTime(dateTime, dateTimeZone, timeZoneTable);
    return result;
  } catch (error) {
    throw error;
  }
}

async function getTime(dateTime, dateTimeZone, timeZoneTable) {
  try {
    if (dateTime == 0 || dateTime == null || dateTime == "" || dateTime.substring(0, 4) == "1900") {
      return "";
    }
    const inputDate = moment(dateTime);
    if (dateTimeZone == "" || dateTimeZone == null) {
      dateTimeZone = "CST";
    }
    const weekNumber = inputDate.isoWeek();
    inputDate.subtract(Number(timeZoneTable[dateTimeZone].HoursAway), 'hours');
    let convertedDate = inputDate.format('YYYY-MM-DDTHH:mm:ss');
    if (weekNumber < 0 && weekNumber > 52) {
      console.info("wrong week number");
    } else if (weekNumber > 11 && weekNumber < 44) {
      convertedDate = convertedDate + "-05:00";
    } else {
      convertedDate = convertedDate + "-06:00";
    }
    if (convertedDate == 0 || convertedDate == null || convertedDate == "" || convertedDate.substring(0, 4) == "1900") {
      return "";
    }
    return convertedDate;
  } catch (error) {
    throw error;
  }
}

async function locationFunc(pKeyValue, houseBill) {
  try {
    let mainArray = [];
    let params = {
      TableName: process.env.TRACKING_NOTES_TABLE,
      IndexName: INDEX_VALUES.TRACKING_NOTES.INDEX,
      KeyConditionExpression: "FK_OrderNo = :orderNo",
      FilterExpression: 'FK_UserId = :FK_UserId',
      ExpressionAttributeValues: {
        ":orderNo": pKeyValue,
        ":FK_UserId": "macropt"
      }
    };
    const data = await ddb.query(params).promise();
    if (data.Items.length !== 0) {
      const trackingNotesLocation = await trackingNotesDataParse(data.Items[0]);
      mainArray.push(trackingNotesLocation);
    }
    let locationParams = {
      TableName: process.env.P44_LOCATION_UPDATE_TABLE,
      KeyConditionExpression: "HouseBillNo = :HouseBillNo",
      ExpressionAttributeValues: {
        ":HouseBillNo": houseBill
      }
    };
    const locationdata = await ddb.query(locationParams).promise();
    await Promise.all(locationdata.Items.map(async (item) => {
      const itemArray = [item.UTCTimeStamp, item.longitude, item.latitude];
      mainArray.push(itemArray);
    }));
    return mainArray;
  } catch (error) {
    throw error;
  }
}

async function trackingNotesDataParse(item) {
  try {
    const note = item.Note;
    const parseArray = note.split(" ");
    const latitude = parseArray[2].split("=")[1];
    const longitute = parseArray[3].split("=")[1];
    return [item.EventDateTime, latitude, longitute];
  } catch (error) {
    throw error;
  }
}

async function getShipmentDate(dateTime) {
  try {
    if (dateTime == 0 || dateTime == null) {
      return "";
    } else {
      return dateTime.substring(0, 10);
    }
  } catch (error) {
    throw error;
  }
}

async function getDescription(orderStatusId, serviceLevelId) {
  if (!orderStatusId || !serviceLevelId) {
    return false;
  }
  const milestoneTableParams = {
    TableName: process.env.MILESTONE_TABLE,
    IndexName: "FK_OrderStatusId-FK_ServiceLevelId",
    KeyConditionExpression: `#pKey = :pKey and #sKey = :sKey`,
    FilterExpression: "IsPublic = :IsPublic",
    ExpressionAttributeNames: {
      "#pKey": "FK_OrderStatusId",
      "#sKey": "FK_ServiceLevelId",
    },
    ExpressionAttributeValues: {
      ":pKey": orderStatusId,
      ":sKey": serviceLevelId,
      ":IsPublic": "Y",
    },
  };
  try {
    const milestoneTableResult = await ddb.query(milestoneTableParams).promise();
    if (milestoneTableResult.Items.length === 0) {
      console.info(`No Description for the FK_OrderStatusId ${orderStatusId} and FK_ServiceLevelId ${serviceLevelId}`);
      return false;
    }
    else {
      return true;
    }
  } catch (error) {
    console.error("Query Error:", error);
    throw error;
  }
}

async function getDynamodbData(value) {
  let timeZoneTable = {};
  const dynamodbData = {};
  try {
    const timeZoneTableParams = {
      TableName: process.env.TIMEZONE_MASTER_TABLE,
    };
    const timeZoneTableResult = await ddb.scan(timeZoneTableParams).promise();
    await Promise.all(
      timeZoneTableResult.Items.map(async (item) => {
        timeZoneTable[item.PK_TimeZoneCode] = item;
      })
    );

    await Promise.all(
      tableValues.map(async (tableValue) => {
        let params = {
          TableName: tableValue.tableName,
          KeyConditionExpression: `#pKey = :pKey`,
          ExpressionAttributeNames: {
            "#pKey": tableValue.pKey,
          },
          ExpressionAttributeValues: {
            ":pKey": value,
          },
        };
        if (tableValue.getValues) {
          params.ProjectionExpression = tableValue.getValues;
        }
        const data = await ddb.query(params).promise();
        dynamodbData[tableValue.tableName] = data.Items;
      })
    );

    const shipmentDescTableParams = {
      TableName: process.env.SHIPMENT_DESC_TABLE,
      KeyConditionExpression: `#pKey = :pKey`,
      ExpressionAttributeNames: {
        "#pKey": "FK_OrderNo",
      },
      ExpressionAttributeValues: {
        ":pKey": value,
      },
    };
    const shipmentDescTableResult = await ddb.query(shipmentDescTableParams).promise();
    dynamodbData[process.env.SHIPMENT_DESC_TABLE] = shipmentDescTableResult.Items;

    const referenceTableParams = {
      TableName: process.env.REFERENCE_TABLE,
      IndexName: process.env.REFERENCE_TABLE_INDEX,
      KeyConditionExpression: `#pKey = :pKey`,
      ExpressionAttributeNames: {
        "#pKey": "FK_OrderNo",
      },
      ExpressionAttributeValues: {
        ":pKey": value,
      },
    };
    const referenceTableResult = await ddb.query(referenceTableParams).promise();
    dynamodbData[process.env.REFERENCE_TABLE] = referenceTableResult.Items;

    const PK_ServiceLevelId = get(dynamodbData, `${process.env.SHIPMENT_HEADER_TABLE}[0].FK_ServiceLevelId`, "");
    if (PK_ServiceLevelId != "") {
      /*
       *Dynamodb data from service level table
       */
      const servicelevelsTableParams = {
        TableName: process.env.SERVICE_LEVEL_TABLE,
        KeyConditionExpression: `#pKey = :pKey`,
        ExpressionAttributeNames: {
          "#pKey": "PK_ServiceLevelId",
        },
        ExpressionAttributeValues: {
          ":pKey": PK_ServiceLevelId,
        },
      };
      const servicelevelsTableResult = await ddb.query(servicelevelsTableParams).promise();
      dynamodbData[process.env.SERVICE_LEVEL_TABLE] = servicelevelsTableResult.Items;
    }

    let shipmentDetailObj = [];
    shipmentDetailObj.push(
      await MappingDataToInsert(dynamodbData, timeZoneTable)
    );
    console.info("shipmentDetailObj", shipmentDetailObj);

    await upsertItem(process.env.SHIPMENT_DETAILS_Collector_TABLE, {
      ...shipmentDetailObj[0],
    });
  } catch (error) {
    console.error("getDynamodbData: ", error);
  }
}

async function checkIfMilestonesPublic(milestones) {
  const filteredMilestoned = [];
  await Promise.all(milestones.map(async milestone => {
    const description = await getDescription(get(milestone, 'FK_OrderStatusId', ""), get(milestone, 'FK_ServiceLevelId', ""));
    if (description) {
      filteredMilestoned.push(milestone);
    }
  }));
  return filteredMilestoned;
}

async function MappingDataToInsert(data, timeZoneTable) {
  const shipmentMilestoneData = data[process.env.SHIPMENT_MILESTONE_TABLE];
  let highestEventDateTimeObject = {};
  if (shipmentMilestoneData.length > 0) {
    highestEventDateTimeObject = shipmentMilestoneData[0];

    for (let i = 1; i < shipmentMilestoneData.length; i++) {
      const currentEventDateTime = new Date(shipmentMilestoneData[i].EventDateTime);
      const highestEventDateTime = new Date(highestEventDateTimeObject.EventDateTime);

      if (currentEventDateTime > highestEventDateTime) {
        highestEventDateTimeObject = shipmentMilestoneData[i];
      }
    }
  } else {
    console.info(`No data found in '${process.env.SHIPMENT_MILESTONE_TABLE} array.`);
  }
  const formattedEventDate = get(highestEventDateTimeObject, 'EventDateTime', '') !== '' ? moment(get(highestEventDateTimeObject, 'EventDateTime', '1900-00-00')).format("YYYY-MM-DD") : '1900-00-00';
  const formattedEventYear = get(highestEventDateTimeObject, 'EventDateTime', '') !== '' ? moment(get(highestEventDateTimeObject, 'EventDateTime', '1900')).format("YYYY") : '1900';
  const formattedOrderDate = get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '') !== '' ? moment(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '1900-00-00')).format("YYYY-MM-DD") : '1900-00-00';
  const formattedOrderYear = get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '') !== '' ? moment(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '1900')).format("YYYY") : '1900';
  let allMilestone;
  if (data[process.env.SHIPMENT_MILESTONE_TABLE]) {
    let milestonePromises = await checkIfMilestonesPublic(data[process.env.SHIPMENT_MILESTONE_TABLE]);
    milestonePromises = milestonePromises.filter(milestone => {
      const statusCode = get(milestone, 'FK_OrderStatusId', "");
      return Object.keys(statusCodes).includes(statusCode);
    });
    milestonePromises = milestonePromises.map(async milestone => {
      const statusCode = get(milestone, 'FK_OrderStatusId', "");
      const statusDescription = get(statusCodes, statusCode, "");
      return {
        statusCode: get(milestone, 'FK_OrderStatusId', ""),
        statusDescription: statusDescription,
        statusTime: await getTime(get(milestone, 'EventDateTime', ""), get(milestone, 'EventTimeZone', ""), timeZoneTable)
      };
    });
    allMilestone = await Promise.all(milestonePromises);
  }
  else {
    console.info(`No milestones found in '${process.env.SHIPMENT_MILESTONE_TABLE} array.`);
  }

  const referencePromises = data[process.env.REFERENCE_TABLE].map(async reference => {
    return {
      refParty: await refParty(get(reference, 'CustomerType', "")),
      refType: get(reference, 'FK_RefTypeId', ""),
      refNumber: get(reference, 'ReferenceNo', "")
    };
  });

  const allReference = await Promise.all(referencePromises);

  const allCustomerIds = data[process.env.CUSTOMER_ENTITLEMENT_TABLE].map(milestone => milestone.CustomerID).join(',');

  const payload = {
    "fileNumber": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].PK_OrderNo`, ""),
    "houseBillNumber": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].Housebill`, ""),
    "masterbill": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].MasterAirWaybill`, ""),
    "shipmentDate": await getShipmentDate(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].ShipmentDateTime`, "")),
    "handlingStation": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].HandlingStation`, ""),
    "originPort": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrgAirport`, ""),
    "destinationPort": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].DestAirport`, ""),
    "shipper": {
      "name": get(data, `${process.env.SHIPPER_TABLE}[0].ShipName`, ""),
      "address": get(data, `${process.env.SHIPPER_TABLE}[0].ShipAddress1`, ""),
      "city": get(data, `${process.env.SHIPPER_TABLE}[0].ShipCity`, ""),
      "state": get(data, `${process.env.SHIPPER_TABLE}[0].FK_ShipState`, ""),
      "zip": get(data, `${process.env.SHIPPER_TABLE}[0].ShipZip`, ""),
      "country": get(data, `${process.env.SHIPPER_TABLE}[0].FK_ShipCountry`, "")
    },
    "consignee": {
      "name": get(data, `${process.env.CONSIGNEE_TABLE}[0].ConName`, ""),
      "address": get(data, `${process.env.CONSIGNEE_TABLE}[0].ConAddress1`, ""),
      "city": get(data, `${process.env.CONSIGNEE_TABLE}[0].ConCity`, ""),
      "state": get(data, `${process.env.CONSIGNEE_TABLE}[0].FK_ConState`, ""),
      "zip": get(data, `${process.env.CONSIGNEE_TABLE}[0].ConZip`, ""),
      "country": get(data, `${process.env.CONSIGNEE_TABLE}[0].FK_ConCountry`, "")
    },
    "pieces": await pieces(get(data, `${process.env.SHIPMENT_DESC_TABLE}`, 0)),
    "actualWeight": await actualWeight(get(data, `${process.env.SHIPMENT_DESC_TABLE}`, 0)),
    "chargeableWeight": await ChargableWeight(get(data, `${process.env.SHIPMENT_DESC_TABLE}`, 0)),
    "weightUOM": await weightUOM(get(data, `${process.env.SHIPMENT_DESC_TABLE}[0].WeightDimension`, "")),
    "pickupTime": await getPickupTime(get(data, `${process.env.SHIPMENT_MILESTONE_DETAIL_TABLE}[0].EventDateTime`, ""), get(data, `${process.env.SHIPMENT_MILESTONE_DETAIL_TABLE}[0].EventTimeZone`, ""), timeZoneTable),
    "estimatedDepartureTime": "",
    "estimatedArrivalTime": await getTime(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].ETADateTime`, ""), get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].ETADateTimeZone`, ""), timeZoneTable),
    "scheduledDeliveryTime": await getTime(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].ScheduledDateTime`, ""), get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].ScheduledDateTimeZone`, ""), timeZoneTable),
    "deliveryTime": await getTime(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].PODDateTime`, ""), get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].PODDateTimeZone`, ""), timeZoneTable),
    "podName": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].PODName`, ""),
    "serviceLevelCode": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].FK_ServiceLevelId`, ""),
    "serviceLevelDescription": get(data, `${process.env.SERVICE_LEVEL_TABLE}[0].ServiceLevel`, ""),
    "customerReference": allReference,
    "milestones": allMilestone,
    "locations": await locationFunc(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].PK_OrderNo`, ""), get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].Housebill`, "")),
    "EventDateTime": get(highestEventDateTimeObject, 'EventDateTime', '1900-00-00 00:00:00.000'),
    "EventDate": formattedEventDate,
    "EventYear": formattedEventYear,
    "OrderDateTime": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '1900-00-00 00:00:00.000'),
    "OrderDate": formattedOrderDate,
    "OrderYear": formattedOrderYear,
    "billNo": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].BillNo`, ""),
    "InsertedTimeStamp": momentTZ.tz("America/Chicago").format("YYYY:MM:DD HH:mm:ss").toString(),
    "customerIds": allCustomerIds,
  };
  return payload;
}

async function upsertItem(tableName, item) {
  const fileNumber = item.fileNumber;
  const customerIds = item.customerIds.split(',');
  try {
    const promises = customerIds.map(async (customerId) => {
      if (!customerId.trim()) {
        console.info("Empty customerId. Skipping upsert for this item.");
        return;
      }
      const existingItem = await ddb.get({
        TableName: tableName,
        Key: {
          fileNumber: fileNumber,
          customerIds: customerId
        },
      }).promise();

      let params;

      if (existingItem.Item) {
        params = {
          TableName: tableName,
          Key: {
            fileNumber: fileNumber,
            customerIds: customerId
          },
          UpdateExpression: 'SET #houseBillNumber = :houseBillNumber, #masterbill = :masterbill, #shipmentDate = :shipmentDate, #handlingStation = :handlingStation, #originPort = :originPort, #destinationPort = :destinationPort, #shipper = :shipper, #consignee = :consignee, #pieces = :pieces, #actualWeight = :actualWeight, #chargeableWeight = :chargeableWeight, #weightUOM = :weightUOM, #pickupTime = :pickupTime, #estimatedDepartureTime = :estimatedDepartureTime, #estimatedArrivalTime = :estimatedArrivalTime, #scheduledDeliveryTime = :scheduledDeliveryTime, #deliveryTime = :deliveryTime, #podName = :podName, #serviceLevelCode = :serviceLevelCode, #serviceLevelDescription = :serviceLevelDescription, #customerReference = :customerReference, #milestones = :milestones, #locations = :locations, #EventDateTime = :EventDateTime, #EventDate = :EventDate, #EventYear = :EventYear, #OrderDateTime = :OrderDateTime, #OrderDate = :OrderDate, #OrderYear = :OrderYear, #billNo = :billNo, #InsertedTimeStamp = :InsertedTimeStamp',
          ExpressionAttributeNames: {
            '#houseBillNumber': 'houseBillNumber',
            '#masterbill': 'masterbill',
            '#shipmentDate': 'shipmentDate',
            '#handlingStation': 'handlingStation',
            '#originPort': 'originPort',
            '#destinationPort': 'destinationPort',
            '#shipper': 'shipper',
            '#consignee': 'consignee',
            '#pieces': 'pieces',
            '#actualWeight': 'actualWeight',
            '#chargeableWeight': 'chargeableWeight',
            '#weightUOM': 'weightUOM',
            '#pickupTime': 'pickupTime',
            '#estimatedDepartureTime': 'estimatedDepartureTime',
            '#estimatedArrivalTime': 'estimatedArrivalTime',
            '#scheduledDeliveryTime': 'scheduledDeliveryTime',
            '#deliveryTime': 'deliveryTime',
            '#podName': 'podName',
            '#serviceLevelCode': 'serviceLevelCode',
            '#serviceLevelDescription': 'serviceLevelDescription',
            '#customerReference': 'customerReference',
            '#milestones': 'milestones',
            '#locations': 'locations',
            '#EventDateTime': 'EventDateTime',
            '#EventDate': 'EventDate',
            '#EventYear': 'EventYear',
            '#OrderDateTime': 'OrderDateTime',
            '#OrderDate': 'OrderDate',
            '#OrderYear': 'OrderYear',
            '#billNo': 'billNo',
            '#InsertedTimeStamp': 'InsertedTimeStamp',
          },
          ExpressionAttributeValues: {
            ':houseBillNumber': item.houseBillNumber,
            ':masterbill': item.masterbill,
            ':shipmentDate': item.shipmentDate,
            ':handlingStation': item.handlingStation,
            ':originPort': item.originPort,
            ':destinationPort': item.destinationPort,
            ':shipper': item.shipper,
            ':consignee': item.consignee,
            ':pieces': item.pieces,
            ':actualWeight': item.actualWeight,
            ':chargeableWeight': item.chargeableWeight,
            ':weightUOM': item.weightUOM,
            ':pickupTime': item.pickupTime,
            ':estimatedDepartureTime': item.estimatedDepartureTime,
            ':estimatedArrivalTime': item.estimatedArrivalTime,
            ':scheduledDeliveryTime': item.scheduledDeliveryTime,
            ':deliveryTime': item.deliveryTime,
            ':podName': item.podName,
            ':serviceLevelCode': item.serviceLevelCode,
            ':serviceLevelDescription': item.serviceLevelDescription,
            ':customerReference': item.customerReference,
            ':milestones': item.milestones,
            ':locations': item.locations,
            ':EventDateTime': item.EventDateTime,
            ':EventDate': item.EventDate,
            ':EventYear': item.EventYear,
            ':OrderDateTime': item.OrderDateTime,
            ':OrderDate': item.OrderDate,
            ':OrderYear': item.OrderYear,
            ':billNo': item.billNo,
            ':InsertedTimeStamp': item.InsertedTimeStamp,
          },
        };
        console.info("Updated");
        return ddb.update(params).promise();
      } else {
        // Modify params to include customerIds
        item.customerIds = customerId;
        params = {
          TableName: tableName,
          Item: item,
        };
        console.info("Inserted");
        return ddb.put(params).promise();
      }
    });

    await Promise.all(promises);

    return "Successfully upserted all items";
  } catch (e) {
    console.error("Put Item Error: ", e);
    throw "PutItemError";
  }
}

module.exports = { refParty, pieces, actualWeight, ChargableWeight, weightUOM, getTime, locationFunc, getShipmentDate, getPickupTime, upsertItem, MappingDataToInsert, getDynamodbData };