'use strict';

const AWS = require('aws-sdk');
const _ = require('lodash');
const moment = require('moment-timezone');

const sns = new AWS.SNS();
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const ses = new AWS.SES();
// Constants from environment variables
const {
  STATUS_TABLE,
  CONSOL_STOP_HEADERS,
  CONSOLE_STATUS_TABLE,
  OMNI_NO_REPLY_EMAIL,
  // eslint-disable-next-line no-unused-vars
  OMNI_DEV_EMAIL,
  SHIPMENT_APAR_TABLE,
  SHIPMENT_APAR_INDEX_KEY_NAME,
  SHIPMENT_HEADER_TABLE,
  LIVE_SNS_TOPIC_ARN,
} = process.env;

// Constants from shared files
const { STATUSES } = require('../shared/constants/204_create_shipment');
const {
  updateOrders,
  getOrders,
  getLocationId,
  createLocation,
} = require('../shared/204-payload-generator/apis');
const { getCstTime } = require('../shared/204-payload-generator/helper');

let functionName;
module.exports.handler = async (event, context) => {
  functionName = _.get(context, 'functionName');
  console.info('🙂 -> file: index.js:8 -> functionName:', functionName);
  try {
    console.info('Event:', event);

    await Promise.all(_.map(event.Records, processRecord));
  } catch (error) {
    console.error('Error processing records:', error);
    throw error;
  }
};

async function processRecord(record) {
  try {
    const body = JSON.parse(_.get(record, 'body', ''));
    const message = JSON.parse(_.get(body, 'Message', ''));

    const newImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'NewImage', {}));
    const oldImage = AWS.DynamoDB.Converter.unmarshall(_.get(message, 'OldImage', {}));
    const dynamoTableName = _.get(message, 'dynamoTableName', '');

    if (dynamoTableName === CONSOL_STOP_HEADERS) {
      await handleUpdatesforMt(newImage, oldImage);
    } else {
      await handleUpdatesForP2P(newImage, oldImage);
    }
  } catch (error) {
    console.error('Error processing record:', error);
    throw error;
  }
}

async function publishSNSTopic({ message, stationCode, subject }) {
  try {
    const params = {
      TopicArn: LIVE_SNS_TOPIC_ARN,
      Subject: subject,
      Message: `An error occurred in ${functionName}: ${message}`,
      MessageAttributes: {
        stationCode: {
          DataType: 'String',
          StringValue: stationCode.toString(),
        },
      },
    };

    await sns.publish(params).promise();
  } catch (error) {
    console.error('Error publishing to SNS topic:', error);
    throw error;
  }
}

async function handleUpdatesForP2P(newImage, oldImage) {
  const orderNo = _.get(newImage, 'FK_OrderNo');
  const consolNo = _.get(newImage, 'ConsolNo');
  try {
    const changedFields = findChangedFields(newImage, oldImage);
    console.info('🚀 ~ file: index.js:57 ~ handleUpdatesForP2P ~ changedFields:', changedFields);
    let shipmentAparData;
    let shipmentHeaderData;
    if (consolNo > 0) {
      shipmentAparData = shipmentAparDataForConsols({ consolNo });
      shipmentHeaderData = await fetchShipmentHeaderData({
        orderNo: _.get(shipmentAparData, '[0]FK_OrderNo'),
      });
      console.info(
        '🚀 ~ file: index.js:66 ~ handleUpdatesForP2P ~ shipmentAparParams:',
        shipmentAparData
      );
    } else {
      shipmentAparData = shipmentAparDataForNC({ orderNo });
      shipmentHeaderData = await fetchShipmentHeaderData({ orderNo });
      console.info(
        '🚀 ~ file: index.js:69 ~ handleUpdatesForP2P ~ shipmentAparParams:',
        shipmentAparData
      );
    }

    console.info(
      '🚀 ~ file: index.js:74 ~ handleUpdatesForP2P ~ shipmentHeaderData:',
      shipmentHeaderData
    );
    const stationCode =
      _.get(shipmentAparData, '[0]FK_HandlingStation', '') ||
      _.get(shipmentHeaderData, '[0]ControllingStation', '');
    console.info('🚀 ~ file: index.js:93 ~ handleUpdatesForP2P ~ stationCode:', stationCode);
    // Query log table
    const logQueryResult = await queryOrderStatusTable(orderNo);
    const status = _.get(logQueryResult, '[0].Status', '');

    if (!_.isEmpty(logQueryResult) && status === STATUSES.FAILED) {
      await updateOrderStatusTable({ orderNo });
    } else if (!_.isEmpty(logQueryResult) && status === STATUSES.SENT) {
      const response = _.get(logQueryResult, '[0].Response', []);
      const id = _.get(response, 'id', '');

      if (_.isEmpty(id)) {
        console.info('Skipping the process as the shipment id is not available');
        return;
      }

      const orderResponse = await getOrders({ id });
      const movements = _.get(orderResponse, 'movements', []);

      if (_.isArray(movements) && movements.length > 1) {
        console.info('There are more than 1 movement arrays in the object.');
        return;
      }

      const brokerageStatus = _.get(movements, '[0].brokerage_status', '');

      if (!_.includes(['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'], brokerageStatus)) {
        console.info('Brokerage status is not OPEN, BOOKED, NEWOMNI, or COVERED.');
        return;
      }
      console.info('Brokerage status is OPEN, BOOKED, NEWOMNI, or COVERED.');

      await updateTimeFieldforConsolAndNonConsole(changedFields);
      const stopsFromResponse = _.cloneDeep(_.get(orderResponse, 'stops', []));
      const updatedStops = await updateStopsFromChangedFields(
        stopsFromResponse,
        changedFields,
        brokerageStatus,
        newImage,
        shipmentHeaderData
      );

      const updatedResponse = _.cloneDeep(orderResponse);
      updatedResponse.stops = updatedStops;

      if (_.isEqual(orderResponse, updatedResponse)) {
        console.info(`Payload is the same as the old payload. Skipping further processing.
            \n Payload: ${JSON.stringify(updatedResponse)}`);
        return;
      }

      await updateOrders({ payload: updatedResponse });
      let updatedFieldsMessage = '';
      if (changedFields) {
        console.info(
          '🚀 ~ file: index.js:112 ~ handleUpdatesForP2P ~ changedFields:',
          changedFields
        );

        if (_.get(changedFields, 'PickupDateTime') || _.get(changedFields, 'PickupTimeRange')) {
          updatedFieldsMessage += `Pick up time has been changed. New pickup time: <strong> ${_.get(newImage, 'PickupDateTime')} </strong>\n`;
        }

        if (_.get(changedFields, 'DeliveryDateTime') || _.get(changedFields, 'DeliveryTimeRange')) {
          updatedFieldsMessage += `Delivery time has been changed. New delivery time: <strong>${_.get(newImage, 'DeliveryDateTime')} </strong>\n`;
        }

        if (
          changedFields.ShipName ||
          changedFields.ShipAddress1 ||
          changedFields.ShipAddress2 ||
          changedFields.ShipCity ||
          changedFields.FK_ShipState ||
          changedFields.ShipZip ||
          changedFields.FK_ShipCountry
        ) {
          updatedFieldsMessage += `Pick up location has been changed. Changed Address: <strong> ${newImage.ShipName}, ${newImage.ShipAddress1}, ${newImage.ShipCity}, ${newImage.FK_ShipState} </strong>\n`;
        }

        if (
          changedFields.ConName ||
          changedFields.ConAddress1 ||
          changedFields.ConAddress2 ||
          changedFields.ConCity ||
          changedFields.FK_ConState ||
          changedFields.ConZip ||
          changedFields.FK_ConCountry
        ) {
          updatedFieldsMessage += `Delivery location has been changed. Changed Address: <strong> ${newImage.ConName}, ${newImage.ConAddress1}, ${newImage.ConCity}, ${newImage.FK_ConState} </strong>\n`;
        }

        console.info(
          '🚀 ~ file: index.js:115 ~ handleUpdatesForP2P ~ updatedFieldsMessage:',
          updatedFieldsMessage
        );
      }

      await sendSESEmail({
        message: updatedFieldsMessage,
        orderId: orderNo,
        consolNo,
        id,
        to: getEmail(stationCode),
      });
      console.info('live ops email:', getEmail(stationCode));
      console.info(
        '🚀 ~ file: index.js:119 ~ handleUpdatesForP2P ~ Sent Update Notification successfully'
      );
      await updateRecord({
        orderNo,
        UpdateCount: _.get(logQueryResult, '[0].UpdateCount', 0),
        updatedResponse,
        oldResponse: _.get(logQueryResult, '[0].UpdatedResponse', {}),
      });
    }
  } catch (error) {
    console.error('Error handling order updates:', error);
    await publishSNSTopic({
      message: `${error.message}`,
      stationCode: 'SUPPORT',
      subject: `An error occurred in 204 Updates function for Consol: ${consolNo} / FK_OrderNo: ${orderNo}`,
    });
    throw error;
  }
}

// eslint-disable-next-line no-unused-vars
async function sendSESEmail({ message, orderId = 0, consolNo = 0, id, to }) {
  try {
    const formattedMessage = message.replace(/\n/g, '<br>');
    const subject = `WT to PB Shipment Updated for #PRO: ${id} / FileNo: ${orderId} / Consol: ${consolNo}`;
    const body = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>204 Updates Notification</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        background-color: #f4f4f4;
        margin: 0;
        padding: 0;
      }
      .container {
        max-width: 600px;
        margin: 20px auto;
        background-color: #ffffff;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
      }
      .header {
        background-color: #007bff;
        color: #ffffff;
        padding: 10px;
        text-align: center;
      }
      .content {
        padding: 20px;
        background-color: #fafafa; /* Lighter background color */
      }
      .message {
        margin-top: 20px;
        padding: 10px;
        background-color: #f9f9f9;
        border-left: 2px solid #007bff; /* Thinner blue line */
        padding-left: 10px; /* Added padding to align the blue line with the text */
      }
    </style>
    </head>
    <body>
    <div class="container">
      <div class="header">
        <h1 style="font-weight: normal;"> WT - PB Updates Notification</h1>
      </div>
      <div class="content">
        <div>#PRO:<strong> ${id}</strong></div>
        <div>FileNo:<strong> ${orderId}</strong></div>
        <div>Consol:<strong> ${consolNo}</strong></div>
        <div class="message">${formattedMessage}</div>
      </div>
    </div>
    </body>
    </html>    
    `;

    const charset = 'UTF-8';
    const sender = OMNI_NO_REPLY_EMAIL;

    const sesParams = {
      Source: sender,
      Destination: {
        // ToAddresses: [to, OMNI_DEV_EMAIL], // uncomment this after testing.
        ToAddresses: ['mohammed.sazeed@bizclouldexperts.com'], // added my email for testing.
      },
      Message: {
        Subject: {
          Data: subject,
          Charset: charset,
        },
        Body: {
          Html: {
            Data: body,
            Charset: charset,
          },
        },
      },
    };

    console.info('🚀 ~ sendSESEmail ~ params:', sesParams);

    await ses.sendEmail(sesParams).promise();
  } catch (error) {
    console.error('Error sending email with SES:', error);
    throw error;
  }
}

async function handleUpdatesforMt(newImage, oldImage) {
  const consolNo = _.get(newImage, 'FK_ConsolNo');
  try {
    const changedFields = findChangedFields(newImage, oldImage);

    console.info('🚀 ~ file: index.js:66 ~ event.Records.map ~ changedFields:', changedFields);
    // Query log table
    const logQueryResult = await queryConsolStatusTable(consolNo);
    if (!_.isEmpty(logQueryResult) && _.get(logQueryResult, '[0]Status') === STATUSES.FAILED) {
      // retrigger the process by updating the status from 'FAILED' to 'PENDING'
      return await updateConsolStatusTable({ consolNo });
    } else if (!_.isEmpty(logQueryResult) && _.get(logQueryResult, '[0]Status') === STATUSES.SENT) {
      const response = _.get(logQueryResult, '[0]Response');

      const id = _.get(response, 'id', '');
      console.info('🚀 ~ file: index.js:46 ~ event.Records.map ~ id:', id);
      if (_.isEmpty(id)) {
        console.info('Skipping the process as the shipment id is not available');
        return true;
      }
      const orderResponse = await getOrders({ id });
      console.info('🚀 ~ file: index.js:43 ~ event.Records.map ~ orderResponse:', orderResponse);

      const movements = _.get(orderResponse, 'movements');

      // Check if movements is an array and if its length is greater than 1
      if (_.isArray(movements) && movements.length > 1) {
        console.info('There are more than 1 movement arrays in the object.');
        return true;
      }
      console.info('There are only 1 movement arrays in the object.');
      // check if the movements array has brokerage_status as  "OPEN", "BOOKED", "NEWOMNI", or "COVERED"
      const brokerageStatus = _.get(movements, '[0].brokerage_status', '');
      if (!_.includes(['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'], brokerageStatus)) {
        console.info('Brokerage status is not OPEN, BOOKED, NEWOMNI, or COVERED.');
        return true;
      }
      console.info('Brokerage status is OPEN, BOOKED, NEWOMNI, or COVERED.');
      // update time fields if there are any changes.
      await updatetimeFields(changedFields, newImage);

      const stopsFromResponse = _.cloneDeep(_.get(orderResponse, 'stops', []));

      console.info(
        '🚀 ~ file: index.js:81 ~ event.Records.map ~ stopsFromResponse:',
        stopsFromResponse
      );
      const updatedStops = await updateStopsForMt(
        stopsFromResponse,
        changedFields,
        brokerageStatus,
        newImage
      );
      console.info('🚀 ~ file: index.js:82 ~ event.Records.map ~ updatedStops:', updatedStops);

      // In the response we need to update the stops array and need to send the entire response with the updates stops array
      const updatedResponse = _.cloneDeep(orderResponse);
      updatedResponse.stops = updatedStops;

      if (_.isEqual(orderResponse, updatedResponse)) {
        console.info(`Payload is the same as the old payload. Skipping further processing.
            \n Payload: ${JSON.stringify(updatedResponse)}`);
        return true;
      }

      await updateOrders({ payload: updatedResponse });
      await updateRecordForMt({
        consolNo,
        UpdateCount: _.get(logQueryResult, '[0].UpdateCount', 0),
        updatedResponse,
        oldResponse: _.get(logQueryResult, '[0].UpdatedResponse', {}),
      });
    }
    return true;
  } catch (error) {
    console.error('Error handling consolidation updates:', error);
    await publishSNSTopic({
      message: `${error.message}`,
      stationCode: 'SUPPORT',
      subject: `An error occurred in 204 Updates function for ConsolNo: ${consolNo} `,
    });
    throw error;
  }
}

function findChangedFields(newImage, oldImage) {
  const changedFields = {};
  Object.keys(newImage).forEach((key) => {
    if (newImage[key] !== oldImage[key]) {
      changedFields[key] = newImage[key];
    }
  });
  return changedFields;
}

async function updateTimeFieldforConsolAndNonConsole(changedFields) {
  if (
    _.get(changedFields, 'PickupTimeRange') ||
    _.get(changedFields, 'PickupDateTime') ||
    _.get(changedFields, 'DeliveryDateTime') ||
    _.get(changedFields, 'DeliveryTimeRange')
  ) {
    if (changedFields.PickupDateTime) {
      changedFields.PickupDateTime = await getCstTime({
        datetime: _.get(changedFields, 'PickupDateTime'),
      });
    }
    if (changedFields.PickupTimeRange) {
      changedFields.PickupTimeRange = await getCstTime({
        datetime: _.get(changedFields, 'PickupTimeRange'),
      });
    }
    if (changedFields.DeliveryDateTime) {
      changedFields.DeliveryDateTime = await getCstTime({
        datetime: _.get(changedFields, 'DeliveryDateTime'),
      });
    }
    if (changedFields.DeliveryTimeRange) {
      changedFields.DeliveryTimeRange = await getCstTime({
        datetime: _.get(changedFields, 'DeliveryTimeRange'),
      });
    }
  }
}

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
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying status table:', error);
    throw error;
  }
};

async function updateOrderStatusTable({ orderNo }) {
  const updateParams = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':status': STATUSES.PENDING,
      ':resetCount': 0,
    },
  };
  console.info('Update status table parameters:', updateParams);
  try {
    await dynamoDb.update(updateParams).promise();
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}

async function updateRecord({ orderNo, UpdateCount, updatedResponse, oldResponse }) {
  const timestamp = moment.tz('America/Chicago').format();
  // Create a new object for the updated response with the current timestamp
  oldResponse[timestamp] = updatedResponse;
  const updateParams = {
    TableName: STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
    UpdateExpression: 'set UpdateCount = :updatecount, #updatedresponse = :updatedresponse',
    ExpressionAttributeNames: {
      '#updatedresponse': 'UpdatedResponse',
    },
    ExpressionAttributeValues: {
      ':updatedresponse': oldResponse,
      ':updatecount': UpdateCount + 1,
    },
  };

  console.info('🚀 ~ file: index.js:145 ~ updateRecord ~ updateParams:', updateParams);
  console.info('Update status table parameters:', updateParams);

  try {
    await dynamoDb.update(updateParams).promise();
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}

async function updateStopsFromChangedFields(
  stops,
  changedFields,
  brokerageStatus,
  newImage,
  shipmentHeaderData
) {
  for (const stop of stops) {
    // eslint-disable-next-line camelcase
    const { order_sequence } = stop;

    // eslint-disable-next-line camelcase
    if (order_sequence === 1) {
      await updatePickupFields(stop, changedFields, brokerageStatus, newImage, shipmentHeaderData);
      // eslint-disable-next-line camelcase
    } else if (order_sequence === 2) {
      await updateDeliveryFields(
        stop,
        changedFields,
        brokerageStatus,
        newImage,
        shipmentHeaderData
      );
    }
  }

  return stops;
}

async function updatePickupFields(
  stop,
  changedFields,
  brokerageStatus,
  newImage,
  shipmentHeaderData
) {
  const isBrokerageStatusOBNC = ['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'].includes(brokerageStatus);
  const isBrokerageStatusON = ['OPEN', 'NEWOMNI'].includes(brokerageStatus);

  if (isBrokerageStatusOBNC) {
    updateONBCPickupFields(stop, changedFields);
  }

  if (isBrokerageStatusON) {
    if (
      changedFields.ShipName ||
      changedFields.ShipAddress1 ||
      changedFields.ShipAddress2 ||
      changedFields.ShipCity ||
      changedFields.FK_ShipState ||
      changedFields.ShipZip ||
      changedFields.FK_ShipCountry
    ) {
      await updateONPickupFields(stop, changedFields, newImage, shipmentHeaderData);
    }
  }
}

async function updateDeliveryFields(
  stop,
  changedFields,
  brokerageStatus,
  newImage,
  shipmentHeaderData
) {
  const isBrokerageStatusOBNC = ['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'].includes(brokerageStatus);
  const isBrokerageStatusON = ['OPEN', 'NEWOMNI'].includes(brokerageStatus);

  if (isBrokerageStatusOBNC) {
    updateONBCDeliveryFields(stop, changedFields);
  }

  if (isBrokerageStatusON) {
    if (
      changedFields.ConName ||
      changedFields.ConAddress1 ||
      changedFields.ConAddress2 ||
      changedFields.ConCity ||
      changedFields.FK_ConState ||
      changedFields.ConZip ||
      changedFields.FK_ConCountry
    ) {
      await updateONDeliveryFields(stop, newImage, shipmentHeaderData);
    }
  }
}

function updateONBCPickupFields(stop, changedFields) {
  _.forEach(changedFields, (value, key) => {
    if (key === 'PickupDateTime') {
      stop.sched_arrive_early = value;
    } else if (key === 'PickupTimeRange') {
      stop.sched_arrive_late = value;
    } else if (key === 'ShipContact') {
      stop.contact_name = value;
    } else if (key === 'ShipPhone') {
      stop.phone = value;
    }
  });
}

async function updateONPickupFields(stopData, newImage, shipmentHeaderData) {
  const houseBills = _.join(_.map(shipmentHeaderData, 'Housebill'));
  console.info('🚀 ~ file: index.js:570 ~ updateONPickupFields ~ houseBills:', houseBills);
  const locationId = await getLocationId(
    newImage.ShipName,
    newImage.ShipAddress1,
    newImage.ShipAddress2,
    newImage.FK_ShipState
  );

  if (!locationId) {
    const newLocation = await createLocation({
      data: {
        __type: 'location',
        company_id: 'TMS',
        address1: newImage.ShipAddress1,
        address2: newImage.ShipAddress2,
        city_name: newImage.ShipCity,
        is_active: true,
        name: newImage.ShipName,
        state: newImage.FK_ShipState,
        zip_code: newImage.ShipZip,
      },
      orderId: newImage.FK_OrderNo,
      country: newImage.FK_ShipCountry,
      consolNo: newImage.FK_ConsolNo,
      houseBill: houseBills,
    });

    stopData.location_id = newLocation;
  } else {
    stopData.location_id = locationId;
  }

  _.unset(stopData, 'address');
  _.unset(stopData, 'address2');
  _.unset(stopData, 'city_name');
  _.unset(stopData, 'state');
  _.unset(stopData, 'zip_code');
  _.unset(stopData, 'city_id');
  _.unset(stopData, 'location_name');
  _.unset(stopData, 'longitude');
  _.unset(stopData, 'latitude');
  _.unset(stopData, 'zone_id');
}

function updateONBCDeliveryFields(stop, changedFields) {
  _.forEach(changedFields, (value, key) => {
    if (key === 'ConContact') {
      stop.contact_name = value;
    } else if (key === 'ConPhone') {
      stop.phone = value;
    } else if (key === 'DeliveryDateTime') {
      stop.sched_arrive_early = value;
    } else if (key === 'DeliveryTimeRange') {
      stop.sched_arrive_late = value;
    }
  });
}

async function updateONDeliveryFields(stopData, newImage, shipmentHeaderData) {
  const houseBills = _.join(_.map(shipmentHeaderData, 'Housebill'));
  console.info('🚀 ~ file: index.js:696 ~ updateONDeliveryFields ~ houseBills:', houseBills)
  const locationId = await getLocationId(
    newImage.ConName,
    newImage.ConAddress1,
    newImage.ConAddress2,
    newImage.FK_ConState
  );
  if (!locationId) {
    const newLocation = await createLocation({
      data: {
        __type: 'location',
        company_id: 'TMS',
        address1: newImage.ConAddress1,
        address2: newImage.ConAddress2,
        city_name: newImage.ConCity,
        is_active: true,
        name: newImage.ConName,
        state: newImage.FK_ConState,
        zip_code: newImage.ConZip,
      },
      orderId: newImage.FK_OrderNo,
      country: newImage.FK_ConCountry,
      consolNo: newImage.FK_ConsolNo,
      houseBill: houseBills,
    });

    stopData.location_id = newLocation;
  } else {
    stopData.location_id = locationId;
  }
  _.unset(stopData, 'address');
  _.unset(stopData, 'address2');
  _.unset(stopData, 'city_name');
  _.unset(stopData, 'state');
  _.unset(stopData, 'zip_code');
  _.unset(stopData, 'city_id');
  _.unset(stopData, 'location_name');
  _.unset(stopData, 'longitude');
  _.unset(stopData, 'latitude');
  _.unset(stopData, 'zone_id');
}
async function updateRecordForMt({ consolNo, UpdateCount, updatedResponse, oldResponse }) {
  const timestamp = moment.tz('America/Chicago').format();
  oldResponse[timestamp] = updatedResponse;
  const updateParams = {
    TableName: CONSOLE_STATUS_TABLE,
    Key: { ConsolNo: consolNo },
    UpdateExpression: 'set UpdateCount = :updatecount, #updatedresponse = :updatedresponse',
    ExpressionAttributeNames: {
      '#updatedresponse': 'UpdatedResponse',
    },
    ExpressionAttributeValues: {
      ':updatedresponse': oldResponse,
      ':updatecount': UpdateCount + 1,
    },
  };

  console.info('🚀 ~ file: index.js:145 ~ updateRecord ~ updateParams:', updateParams);
  console.info('Update status table parameters:', updateParams);

  try {
    await dynamoDb.update(updateParams).promise();
  } catch (error) {
    console.error('Error updating status table:', error);
    throw error;
  }
}

async function updateStopsForMt(stopsFromResponse, changedFields, brokerageStatus, newImage) {
  try {
    const { ConsolStopNumber } = newImage;
    const orderSequence = _.toNumber(ConsolStopNumber) + 1;

    for (const stop of stopsFromResponse) {
      if (stop.order_sequence === orderSequence) {
        await updateChangedFieldsforMt(stop, changedFields, brokerageStatus, newImage);
        break;
      }
    }
    return stopsFromResponse;
  } catch (error) {
    console.error(error);
    throw error;
  }
}

async function updateChangedFieldsforMt(stop, changedFields, brokerageStatus, newImage) {
  const isBrokerageStatusOBNC = ['OPEN', 'BOOKED', 'NEWOMNI', 'COVERED'].includes(brokerageStatus);
  const isBrokerageStatusON = ['OPEN', 'NEWOMNI'].includes(brokerageStatus);

  if (isBrokerageStatusOBNC) {
    updateONBCFields(stop, changedFields);
  }

  if (isBrokerageStatusON) {
    if (
      changedFields.ConsolStopName ||
      changedFields.ConsolStopAddress1 ||
      changedFields.ConsolStopAddress2 ||
      changedFields.ConsolStopCity ||
      changedFields.FK_ConsolStopState ||
      changedFields.ConsolStopZip ||
      changedFields.FK_ConsolStopCountry
    ) {
      await updateONFields(stop, newImage);
    }
  }
}

function updateONBCFields(stop, changedFields) {
  _.forEach(changedFields, (value, key) => {
    if (key === 'ConsolStopContact') {
      stop.contact_name = value;
    } else if (key === 'ConsolStopPhone') {
      stop.phone = value;
    } else if (key === 'ConsolStopTimeBegin') {
      stop.sched_arrive_early = value;
    } else if (key === 'ConsolStopTimeEnd') {
      stop.sched_arrive_late = value;
    }
  });
}

async function updateONFields(stop, newImage) {
  const locationId = await getLocationId(
    newImage.ConsolStopName,
    newImage.ConsolStopAddress1,
    newImage.ConsolStopAddress2,
    newImage.FK_ConsolStopState
  );

  if (!locationId) {
    const newLocation = await createLocation({
      data: {
        __type: 'location',
        company_id: 'TMS',
        address1: newImage.ConsolStopAddress1,
        address2: newImage.ConsolStopAddress2,
        is_active: true,
        name: newImage.ConsolStopName,
        state: newImage.FK_ConsolStopState,
        city_name: newImage.ConsolStopCity,
        zip_code: newImage.ConsolStopZip,
      },
      consolNo: newImage.FK_ConsolNo,
      country: newImage.FK_ConsolStopCountry,
    });

    stop.location_id = newLocation;
  } else {
    stop.location_id = locationId;
  }

  _.unset(stop, 'address');
  _.unset(stop, 'address2');
  _.unset(stop, 'city_name');
  _.unset(stop, 'state');
  _.unset(stop, 'zip_code');
  _.unset(stop, 'city_id');
  _.unset(stop, 'location_name');
  _.unset(stop, 'longitude');
  _.unset(stop, 'latitude');
  _.unset(stop, 'zone_id');
}

async function updatetimeFields(changedFields, newImage) {
  if (
    _.get(changedFields, 'ConsolStopTimeBegin') ||
    _.get(changedFields, 'ConsolStopTimeEnd') ||
    _.get(changedFields, 'ConsolStopDate')
  ) {
    if (changedFields.ConsolStopTimeBegin || _.get(changedFields, 'ConsolStopDate')) {
      const consolStopDate = _.get(newImage, 'ConsolStopDate');
      console.info('🚀 ~ file: index.js:611 ~ updatetimeFields ~ consolStopDate:', consolStopDate);
      const consolStopTimeBegin = _.get(newImage, 'ConsolStopTimeBegin');
      console.info(
        '🚀 ~ file: index.js:613 ~ updatetimeFields ~ consolStopTimeBegin:',
        consolStopTimeBegin
      );
      const datePart = consolStopDate.split(' ')[0];
      const timePart = consolStopTimeBegin.split(' ')[1];

      if (datePart && timePart) {
        const datetime = `${datePart} ${timePart}`;
        console.info('🚀 ~ file: index.js:629 ~ updatetimeFields ~ datetime:', datetime);
        changedFields.ConsolStopTimeBegin = await getCstTime({ datetime });
        console.info(
          '🚀 ~ file: index.js:629 ~ updatetimeFields ~ changedFields.ConsolStopTimeBegin:',
          changedFields.ConsolStopTimeBegin
        );
      }
    }
    if (changedFields.ConsolStopTimeEnd || _.get(changedFields, 'ConsolStopDate')) {
      const consolStopDate = _.get(newImage, 'ConsolStopDate');
      console.info('🚀 ~ file: index.js:633 ~ updatetimeFields ~ consolStopDate:', consolStopDate);
      const consolStopTimeEnd = _.get(newImage, 'ConsolStopTimeEnd');
      console.info(
        '🚀 ~ file: index.js:635 ~ updatetimeFields ~ consolStopTimeEnd:',
        consolStopTimeEnd
      );
      const datePart = consolStopDate.split(' ')[0];
      const timePart = consolStopTimeEnd.split(' ')[1];

      if (datePart && timePart) {
        const datetime = `${datePart} ${timePart}`;
        console.info('🚀 ~ file: index.js:653 ~ updatetimeFields ~ datetime:', datetime);
        changedFields.ConsolStopTimeEnd = await getCstTime({ datetime });
        console.info(
          '🚀 ~ file: index.js:642 ~ updatetimeFields ~ changedFields.ConsolStopTimeEnd:',
          changedFields.ConsolStopTimeEnd
        );
      }
    }
    console.info('updated the time fields successfully');
  }
}

async function queryConsolStatusTable(consolNo) {
  try {
    const queryParams = {
      TableName: CONSOLE_STATUS_TABLE,
      KeyConditionExpression: 'ConsolNo = :consolNo',
      ExpressionAttributeValues: {
        ':consolNo': consolNo,
      },
    };
    console.info('Query status table parameters:', queryParams);
    const result = await dynamoDb.query(queryParams).promise();
    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('🚀 ~ file: index.js:460 ~ queryConsolStatusTable ~ error:', error);
    throw error;
  }
}

async function updateConsolStatusTable({ consolNo }) {
  try {
    const updateParams = {
      TableName: CONSOLE_STATUS_TABLE,
      Key: { ConsolNo: consolNo },
      UpdateExpression: 'set #Status = :status, ResetCount = :resetCount',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': STATUSES.PENDING,
        ':resetCount': 0,
      },
    };
    console.info('Update status table parameters:', updateParams);
    await dynamoDb.update(updateParams).promise();
    return 'success';
  } catch (error) {
    console.error('🚀 ~ file: index.js:481 ~ updateConsolStatusTable ~ error:', error);
    throw error;
  }
}

const stationEmailMapping = {
  ACN: 'brokerageops4@omnilogistics.com',
  LAX: 'brokerageops9@omnilogistics.com',
  LGB: 'brokerageops9@omnilogistics.com',
  IAH: 'brokerageops4@omnilogistics.com',
  BNA: 'brokerageops4@omnilogistics.com',
  AUS: 'brokerageops4@omnilogistics.com',
  COE: 'brokerageops4@omnilogistics.com',
  PHL: 'brokerageops9@omnilogistics.com',
  ORD: 'brokerageops5@omnilogistics.com',
  SAT: 'brokerageops5@omnilogistics.com',
  BOS: 'brokerageops4@omnilogistics.com',
  SLC: 'brokerageops5@omnilogistics.com',
  ATL: 'brokerageops4@omnilogistics.com',
  BRO: 'brokerageops4@omnilogistics.com',
  CMH: 'brokerageops4@omnilogistics.com',
  CVG: 'brokerageops4@omnilogistics.com',
  DAL: 'brokerageops4@omnilogistics.com',
  DTW: 'brokerageops4@omnilogistics.com',
  EXP: 'brokerageops4@omnilogistics.com',
  GSP: 'brokerageops4@omnilogistics.com',
  LRD: 'brokerageops4@omnilogistics.com',
  MKE: 'brokerageops4@omnilogistics.com',
  MSP: 'brokerageops4@omnilogistics.com',
  PHX: 'brokerageops4@omnilogistics.com',
  TAN: 'brokerageops4@omnilogistics.com',
  YYZ: 'brokerageops4@omnilogistics.com',
  DFW: 'brokerageops4@omnilogistics.com',
  IND: 'brokerageops4@omnilogistics.com',
  ELP: 'brokerageops6@omnilogistics.com',
  PIT: 'brokerageops5@omnilogistics.com',
  MFE: 'brokerageops4@omnilogistics.com',
  OLH: 'brokerageops5@omnilogistics.com',
  OTR: 'brokerageops7@omnilogistics.com',
  PDX: 'brokerageops4@omnilogistics.com',
  SAN: 'brokerageops5@omnilogistics.com',
  SFO: 'brokerageops5@omnilogistics.com',
};

function getEmail(stationCode) {
  return stationEmailMapping[stationCode];
}

async function fetchShipmentHeaderData({ orderNo }) {
  try {
    const params = {
      TableName: SHIPMENT_HEADER_TABLE,
      KeyConditionExpression: 'PK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    };
    console.info('🙂 -> file: index.js:216 -> fetchOrderData -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return _.get(data, 'Items', []);
  } catch (err) {
    console.error('🚀 ~ file: index.js:263 ~ fetchOrderData ~ err:', err);
    throw err;
  }
}

async function shipmentAparDataForNC({ orderNo }) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      KeyConditionExpression: 'FK_OrderNo = :orderNo',
      ExpressionAttributeValues: {
        ':orderNo': orderNo,
      },
    };
    console.info('🙂 -> file: index.js:216 -> fetchOrderData -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return _.get(data, 'Items', []);
  } catch (err) {
    console.error('🚀 ~ file: index.js:263 ~ fetchOrderData ~ err:', err);
    throw err;
  }
}

async function shipmentAparDataForConsols({ consolNo }) {
  try {
    const params = {
      TableName: SHIPMENT_APAR_TABLE,
      IndexName: SHIPMENT_APAR_INDEX_KEY_NAME,
      KeyConditionExpression: 'ConsolNo = :ConsolNo',
      FilterExpression: 'Consolidation = :consolidation',
      ExpressionAttributeValues: {
        ':ConsolNo': String(consolNo),
        ':consolidation': 'N',
      },
    };
    console.info('🙂 -> file: index.js:216 -> fetchOrderData -> params:', params);
    const data = await dynamoDb.query(params).promise();
    console.info('🙂 -> file: index.js:138 -> fetchItemFromTable -> data:', data);
    return _.get(data, 'Items', []);
  } catch (err) {
    console.error('🚀 ~ file: index.js:263 ~ fetchOrderData ~ err:', err);
    throw err;
  }
}
