const AWS = require("aws-sdk");
const moment = require('moment');
const { get } = require('lodash');
const ddb = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();
const athena = new AWS.Athena();

const util = require('util');
const setTimeoutPromise = util.promisify(setTimeout);

const tracking_notes_table = process.env.TRACKING_NOTES_TABLE

const { tableValues, weightDimensionValue, INDEX_VALUES, customerTypeValue } = require("../constants/shipment_details");

async function refParty(customerType) {
  try {
    let refParty = get(customerTypeValue, `${customerType}`, null)
    if (refParty == null) {
      refParty = get(customerTypeValue, "default", null)
    }
    return refParty;
  } catch (error) {
    throw error
  }
}
async function pieces(tableValue) {
  try {
    let pieces = 0
    await Promise.all(tableValue.map(async (val) => {
      pieces += val.Pieces
    }))
    return Number(pieces);
  } catch (error) {
    throw error
  }
}
async function actualWeight(tableValue) {
  try {
    let actualWeight = 0
    await Promise.all(tableValue.map(async (val) => {
      actualWeight += Number(val.Weight)
    }))
    return Number(actualWeight);
  } catch (error) {
    throw error
  }
}
async function ChargableWeight(tableValue) {
  try {
    let ChargableWeight = 0
    await Promise.all(tableValue.map(async (val) => {
      ChargableWeight += Number(val.ChargableWeight)
    }))
    return Number(ChargableWeight);
  } catch (error) {
    throw error
  }
}
async function weightUOM(weightDimension) {
  try {
    let weightUOM = get(weightDimensionValue, `${weightDimension}`, null)
    if (weightUOM == null) {
      weightUOM = get(weightDimensionValue, "default", null)
    }
    return weightUOM;
  } catch (error) {
    throw error
  }
}

async function getPickupTime(dateTime, dateTimeZone, timeZoneTable) {
  try {
    const result = await getTime(dateTime, dateTimeZone, timeZoneTable)
    // if (result == 0 || result == null || result == "" || result.substring(0, 4) == "1900") {
    //   return ""
    // }
    return result
  } catch (error) {
    throw error
  }
}

async function getTime(dateTime, dateTimeZone, timeZoneTable) {
  try {
    if (dateTime == 0 || dateTime == null || dateTime == "" || dateTime.substring(0, 4) == "1900") {
      return ""
    }
    const inputDate = moment(dateTime);
    if (dateTimeZone == "" || dateTimeZone == null) {
      dateTimeZone = "CST"
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
      return ""
    }
    return convertedDate;
  } catch (error) {
    throw error
  }
}

async function locationFunc(pKeyValue, houseBill) {
  try {
    let mainArray = []
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
      const trackingNotesLocation = await trackingNotesDataParse(data.Items[0])
      mainArray.push(trackingNotesLocation)
    }
    let locationParams = {
      TableName: process.env.P44_LOCATION_UPDATE_TABLE,
      KeyConditionExpression: "HouseBillNo = :HouseBillNo",
      ExpressionAttributeValues: {
        ":HouseBillNo": houseBill
      }
    }
    const locationdata = await ddb.query(locationParams).promise();
    await Promise.all(locationdata.Items.map(async (item) => {
      const itemArray = [item.UTCTimeStamp, item.longitude, item.latitude]
      mainArray.push(itemArray)
    }))
    return mainArray;
  } catch (error) {
    throw error
  }
}

async function trackingNotesDataParse(item) {
  try {
    const note = item.Note
    const parseArray = note.split(" ")
    const latitude = parseArray[2].split("=")[1]
    const longitute = parseArray[3].split("=")[1]
    return [item.EventDateTime, latitude, longitute]
  } catch (error) {
    throw error
  }
}

async function getShipmentDate(dateTime) {
  try {
    if (dateTime == 0 || dateTime == null) {
      return ""
    } else {
      return dateTime.substring(0, 10)
    }
  } catch (error) {
    throw error
  }
}

async function getDynamodbData(value){
  let mainResponse = {};
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

    const PK_ServiceLevelId = get(dynamodbData,`${process.env.SHIPMENT_HEADER_TABLE}[0].FK_ServiceLevelId`,null);
  
    if (PK_ServiceLevelId != null || PK_ServiceLevelId != "") {
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
      dynamodbData[process.env.SERVICE_LEVEL_TABLE] =servicelevelsTableResult.Items;
    }

    const FK_ServiceLevelId = get(dynamodbData,`${process.env.SHIPMENT_MILESTONE_TABLE}[0].FK_ServiceLevelId`,null);
    const FK_OrderStatusId = get(dynamodbData,`${process.env.SHIPMENT_MILESTONE_TABLE}[0].FK_OrderStatusId`,null);
  
    if (FK_ServiceLevelId == null ||FK_ServiceLevelId == " " ||FK_ServiceLevelId == "" ||FK_OrderStatusId == null ||FK_OrderStatusId == "") {
      console.info("no servicelevelId for ",get(dynamodbData,`${process.env.SHIPMENT_MILESTONE_TABLE}[0].FK_OrderNo `, null ));
    } else {
      /*
       *Dynamodb data from milestone table
       */
      const milestoneTableParams = {
        TableName: process.env.MILESTONE_TABLE,
        KeyConditionExpression: `#pKey = :pKey and #sKey = :sKey`,
        FilterExpression: "IsPublic = :IsPublic",
        ExpressionAttributeNames: {
          "#pKey": "FK_OrderStatusId",
          "#sKey": "FK_ServiceLevelId",
        },
        ExpressionAttributeValues: {
          ":pKey": FK_OrderStatusId,
          ":sKey": FK_ServiceLevelId,
          ":IsPublic": "Y",
        },
      };
      const milestoneTableResult = await ddb.query(milestoneTableParams).promise();
      dynamodbData[process.env.MILESTONE_TABLE] = milestoneTableResult.Items;
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

async function MappingDataToInsert(data, timeZoneTable) {
  const payload = {
    "fileNumber": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].PK_OrderNo`, ""),
    "HouseBillNumber": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].Housebill`, ""),
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
    "customerReference": [
      {
        "refParty": await refParty(get(data, `${process.env.REFERENCE_TABLE}[0].CustomerType`, "")),
        "refType": get(data, `${process.env.REFERENCE_TABLE}[0].FK_RefTypeId`, ""),
        "refNumber": get(data, `${process.env.REFERENCE_TABLE}[0].ReferenceNo`, "")
      }
    ],
    "milestones": [{
      "statusCode": get(data, `${process.env.SHIPMENT_MILESTONE_TABLE}[0].FK_OrderStatusId`, ""),
      "statusDescription": get(data, `${process.env.MILESTONE_TABLE}[0].Description`, ""),
      "statusTime": await getTime(get(data, `${process.env.SHIPMENT_MILESTONE_TABLE}[0].EventDateTime`, ""), get(data, `${process.env.SHIPMENT_MILESTONE_TABLE}[0].EventTimeZone`, ""), timeZoneTable)
    }],
    "locations": await locationFunc(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].PK_OrderNo`, ""), get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].Housebill`, "")),
    "EventDateTime": get(data, `${process.env.SHIPMENT_MILESTONE_TABLE}[0].EventDateTime`, '1900-00-00 00:00:00.000'),
    "EventDate": moment(get(data, `${process.env.SHIPMENT_MILESTONE_TABLE}[0].EventDateTime`, '1900-00-00')).format("YYYY-MM-DD"),
    "EventYear": moment(get(data, `${process.env.SHIPMENT_MILESTONE_TABLE}[0].EventDateTime`, '1900')).format("YYYY"),
    "OrderDateTime": get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '1900-00-00 00:00:00.000'),
    "OrderDate": moment(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '1900-00-00')).format("YYYY-MM-DD"),
    "OrderYear": moment(get(data, `${process.env.SHIPMENT_HEADER_TABLE}[0].OrderDate`, '1900')).format("YYYY"),
  }
    
    const ignoreFields = ['masterbill', 'pickupTime', 'estimatedDepartureTime', 'estimatedArrivalTime', 'scheduledDeliveryTime', 'deliveryTime', 'podName'];
  
    const values = Object.entries(payload).map(([key, value]) => {
      if (ignoreFields.includes(key)) {
        return true; 
      }
      return value;
    });
  
    const hasNullOrEmptyValue = values.some(value => value === null || value === '');
  
    const status = hasNullOrEmptyValue ? 'Pending' : 'Ready';
    payload.status = status;
    return payload;
  }
  
  async function upsertItem(tableName, item) {
    const houseBillNumber = item.HouseBillNumber;
    let params;
  
    try {
      const existingItem = await ddb.get({
        TableName: tableName,
        Key: {
          HouseBillNumber: houseBillNumber,
        },
      }).promise();
  
  
      if (existingItem.Item) {
        params = {
          TableName: tableName,
          Key: {
            HouseBillNumber: houseBillNumber,
          },
          UpdateExpression: 'SET #fileNumber = :fileNumber, #masterbill = :masterbill, #shipmentDate = :shipmentDate, #handlingStation = :handlingStation, #originPort = :originPort, #destinationPort = :destinationPort, #shipper = :shipper, #consignee = :consignee, #pieces = :pieces, #actualWeight = :actualWeight, #chargeableWeight = :chargeableWeight, #weightUOM = :weightUOM, #pickupTime = :pickupTime, #estimatedDepartureTime = :estimatedDepartureTime, #estimatedArrivalTime = :estimatedArrivalTime, #scheduledDeliveryTime = :scheduledDeliveryTime, #deliveryTime = :deliveryTime, #podName = :podName, #serviceLevelCode = :serviceLevelCode, #serviceLevelDescription = :serviceLevelDescription, #customerReference = :customerReference, #milestones = :milestones, #locations = :locations, #EventDateTime = :EventDateTime, #EventDate = :EventDate, #EventYear = :EventYear, #OrderDateTime = :OrderDateTime, #OrderDate = :OrderDate, #OrderYear = :OrderYear, #status = :status',
          ExpressionAttributeNames: {
            '#fileNumber': 'fileNumber',
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
            '#status': 'status',
          },
          ExpressionAttributeValues: {
            ':fileNumber': item.fileNumber,
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
            ':status': item.status,
          },
        };
        console.info("Updated")
        return await ddb.update(params).promise();
      } else {
        params = {
          TableName: tableName,
          Item: item,
        };
        console.info("inserted")
        return await ddb.put(params).promise();
      }
    } catch (e) {
      console.error("Put Item Error: ", e, "\nPut params: ", params);
      throw "PutItemError";
    }
  }
  
module.exports = { refParty, pieces, actualWeight, ChargableWeight, weightUOM, getTime, locationFunc, getShipmentDate, getPickupTime, upsertItem, MappingDataToInsert, getDynamodbData }