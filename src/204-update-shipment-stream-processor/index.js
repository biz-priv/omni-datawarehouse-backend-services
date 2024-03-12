'use strict';
const AWS = require('aws-sdk');
const { get, isEqual } = require('lodash');
const { updateOrders } = require('../shared/204-payload-generator/apis');
const { getCstTime, getTimezone } = require('../shared/204-payload-generator/helper');
const { STATUSES } = require('../shared/constants/204_create_shipment');

const dynamoDb = new AWS.DynamoDB.DocumentClient();

const { STATUS_TABLE, CONFIRMATION_COST, CONFIRMATION_COST_INDEX_KEY_NAME } = process.env;

module.exports.handler = async (event) => {
  await Promise.all(
    event.Records.map(async (record) => {
      const body = JSON.parse(get(record, 'body', ''));
      const message = JSON.parse(get(body, 'Message', ''));
      console.info('🙂 -> file: index.js:9 -> module.exports.handler= -> message:', typeof message);
      const newImage = AWS.DynamoDB.Converter.unmarshall(get(message, 'NewImage', {}));
      console.info('🙂 -> file: index.js:11 -> module.exports.handler= -> newImage:', newImage);
      const oldImage = AWS.DynamoDB.Converter.unmarshall(get(message, 'OldImage', {}));
      console.info('🙂 -> file: index.js:13 -> module.exports.handler= -> oldImage:', oldImage);
      const dynamoTableName = get(message, 'dynamoTableName', '');
      console.info(
        '🚀 ~ file: index.js:16 ~ event.Records.map ~ dynamoTableName:',
        dynamoTableName
      );
      const orderNo = get(newImage, 'FK_OrderNo');

      // Find changed fields using lodash
      const changedFields = {};
      for (const key in newImage) {
        if (!isEqual(newImage[key], oldImage[key])) {
          changedFields[key] = newImage[key];
        }
      }

      console.info('Changed fields:', changedFields);

      if (
        get(changedFields, 'PickupTimeRange') ||
        get(changedFields, 'PickUpDateTime') ||
        get(changedFields, 'DeliveryDateTime') ||
        get(changedFields, 'DeliveryTimeRange')
      ) {
        const { shipperTimeZone, consigneeTimeZone } =
          await getShipperAndConsigneeTimezones(orderNo);
        if (changedFields.PickUpDateTime) {
          changedFields.PickUpDateTime = await getCstTime({
            datetime: get(changedFields, 'PickUpDateTime'),
            timezone: shipperTimeZone,
          });
        }
        if (changedFields.PickupTimeRange) {
          changedFields.PickupTimeRange = await getCstTime({
            datetime: get(changedFields, 'PickupTimeRange'),
            timezone: shipperTimeZone,
          });
        }
        if (changedFields.DeliveryDateTime) {
          changedFields.DeliveryDateTime = await getCstTime({
            datetime: get(changedFields, 'DeliveryDateTime'),
            timezone: consigneeTimeZone,
          });
        }
        if (changedFields.DeliveryTimeRange) {
          changedFields.DeliveryTimeRange = await getCstTime({
            datetime: get(changedFields, 'DeliveryTimeRange'),
            timezone: consigneeTimeZone,
          });
        }
      }

      // Query log table
      const logQueryResult = await queryOrderStatusTable(orderNo);
      if (logQueryResult.length > 0) {
        if (get(logQueryResult, '[0]Status') === STATUSES.FAILED) {
          // retrigger the process by updating the status to 'READY'
          await updateOrderStatusTable({ orderNo });
        } else {
          const response = get(logQueryResult, '[0]Response');
          console.info('🚀 ~ file: index.js:42 ~ event.Records.map ~ Response:', response);
          const stopsFromResponse = get(response, 'stops', []);
          console.info(
            '🚀 ~ file: index.js:45 ~ event.Records.map ~ stopsFromResponse:',
            stopsFromResponse
          );
          const updatedStops = await updateStopsFromChangedFields(stopsFromResponse, changedFields);
          console.info('🚀 ~ file: index.js:54 ~ event.Records.map ~ updatedStops:', updatedStops);
          // In the response we need to update the stops array and need to send the entire response with the updates stops array
          const updatedResponse = { ...response, stops: updatedStops };
          // Send update
          await updateOrders({ payload: updatedResponse });
        }
      } else {
        console.info('Update not found in log table.');
      }
    })
  );
};

const queryOrderStatusTable = async (orderNo) => {
  const orderParam = {
    TableName: STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  console.info('orderParam:', orderParam);
  try {
    const result = await dynamoDb.query(orderParam).promise();
    return get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying status table:', error);
    throw error;
  }
};

async function updateOrderStatusTable({ orderNo }) {
  try {
    const updateParam = {
      TableName: STATUS_TABLE,
      Key: { FK_OrderNo: orderNo },
      UpdateExpression: 'set #Status = :status',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': STATUSES.READY,
      },
    };
    console.info('🙂 -> file: index.js:125 -> updateParam:', updateParam);
    return await dynamoDb.update(updateParam).promise();
  } catch (err) {
    console.info('🙂 -> file: index.js:224 -> err:', err);
    throw err;
  }
}

// Function to update stops based on changed fields
async function updateStopsFromChangedFields(stops, changedFields) {
  for (const stop of stops) {
    // eslint-disable-next-line camelcase
    const { order_sequence } = stop;

    // eslint-disable-next-line camelcase
    if (order_sequence === 1) {
      // Handle Pickup
      updatePickupFields(stop, changedFields);
      // eslint-disable-next-line camelcase
    } else if (order_sequence === 2) {
      // Handle Delivery
      updateDeliveryFields(stop, changedFields);
    }
  }

  return stops;
}

// Function to update pickup fields
function updatePickupFields(stop, changedFields) {
  if (changedFields.ShipName) {
    stop.location_name = changedFields.ShipName;
  }
  if (changedFields.ShipContact) {
    stop.contact_name = changedFields.ShipContact;
  }
  if (changedFields.ShipZip) {
    stop.zip_code = changedFields.ShipZip;
  }
  if (changedFields.ShipCity) {
    stop.city_name = changedFields.ShipCity;
  }
  if (changedFields.ShipPhone) {
    stop.phone = changedFields.ShipPhone;
  }
  if (changedFields.ShipAddress1) {
    stop.address = changedFields.ShipAddress1;
  }
  if (changedFields.ShipAddress2) {
    stop.address2 = changedFields.ShipAddress2;
  }
  if (changedFields.FK_ShipState) {
    stop.state = changedFields.FK_ShipState;
  }
  if (changedFields.PickUpDateTime) {
    stop.sched_arrive_early = changedFields.PickUpDateTime;
  }
  if (changedFields.PickupTimeRange) {
    stop.sched_arrive_late = changedFields.PickupTimeRange;
  }
}

// Function to update delivery fields
function updateDeliveryFields(stop, changedFields) {
  if (changedFields.ConAddress1) {
    stop.address = changedFields.ConAddress1;
  }
  if (changedFields.ConAddress2) {
    stop.address2 = changedFields.ConAddress2;
  }
  if (changedFields.ConCity) {
    stop.city_name = changedFields.ConCity;
  }
  if (changedFields.ConContact) {
    stop.contact_name = changedFields.ConContact;
  }
  if (changedFields.ConName) {
    stop.location_name = changedFields.ConName;
  }
  if (changedFields.ConPhone) {
    stop.phone = changedFields.ConPhone;
  }
  if (changedFields.DeliveryDateTime) {
    stop.sched_arrive_early = changedFields.DeliveryDateTime;
  }
  if (changedFields.DeliveryTimeRange) {
    stop.sched_arrive_late = changedFields.DeliveryTimeRange;
  }
  if (changedFields.FK_ConState) {
    stop.state = changedFields.FK_ConState;
  }
  if (changedFields.ConZip) {
    stop.zip_code = changedFields.ConZip;
  }
}

async function getShipperAndConsigneeTimezones(orderNo) {
  // Query DynamoDB table to get the necessary shipper data
  const Params = {
    TableName: CONFIRMATION_COST,
    IndexName: CONFIRMATION_COST_INDEX_KEY_NAME,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  const stateData = await dynamoDb.query(Params).promise();

  // Extract required data from stateData for shipper
  const shipperCity = get(stateData, 'Items[0].ShipCity', '');
  const shipperState = get(stateData, 'Items[0].FK_ShipState', '');
  const shipperCountry = get(stateData, 'Items[0].FK_ShipCountry', '');
  const shipperAddress1 = get(stateData, 'Items[0].ShipAddress1', '');
  // Extract required data from stateData for consignee
  const consigneeCity = get(stateData, 'Items[0].ConCity', '');
  const consigneeState = get(stateData, 'Items[0].FK_ConState', '');
  const consigneeCountry = get(stateData, 'Items[0].FK_ConCountry', '');
  const consigneeAddress1 = get(stateData, 'Items[0].ConAddress1', '');

  // Call getTimezone function with extracted data for shipper
  const shipperTimeZone = await getTimezone({
    stopCity: shipperCity,
    state: shipperState,
    country: shipperCountry,
    address1: shipperAddress1,
  });

  // Call getTimezone function with extracted data for consignee
  const consigneeTimeZone = await getTimezone({
    stopCity: consigneeCity,
    state: consigneeState,
    country: consigneeCountry,
    address1: consigneeAddress1,
  });

  return { shipperTimeZone, consigneeTimeZone };
}
