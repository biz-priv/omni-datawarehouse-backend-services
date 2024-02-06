'use strict';

const AWS = require('aws-sdk');
const axios = require('axios');

const dynamodb = new AWS.DynamoDB.DocumentClient();
const _ = require('lodash');
const moment = require('moment-timezone');
const { STATUSES } = require('../shared/constants/204_create_shipment');

const { CONSOL_STATUS_TABLE } = process.env;

module.exports.handler = async (event) => {
  console.info('🚀 ~ file: index.js:9 ~ event:', event);

  try {
    const promises = _.get(event, 'Records', []).map(async (record) => {
      const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
      console.info('🙂 -> file: index.js:17 -> shipmentAparData:', shipmentAparData);

      const orderId = _.get(shipmentAparData, 'FK_OrderNo');
      console.info('🙂 -> file: index.js:23 -> orderId:', orderId);

      const consolNo = parseInt(_.get(shipmentAparData, 'ConsolNo', null), 10);
      console.info('🙂 -> file: index.js:23 -> consolNo:', consolNo);

      const serviceId = _.get(shipmentAparData, 'FK_ServiceId');
      console.info('🙂 -> file: index.js:26 -> serviceLevelId:', serviceId);

      const vendorId = _.get(shipmentAparData, 'FK_VendorId', '').toUpperCase();
      console.info('🙂 -> file: index.js:33 -> vendorId:', vendorId);

      const consolidation = _.get(shipmentAparData, 'Consolidation', '');

      if (!isNaN(consolNo) && consolNo !== null && consolidation === 'N' && serviceId === 'MT') {
        const orderIds = await makeGetRequest(consolNo);
        // const orderIds = ['5345530', '5345531'];
        const orderIdCount = _.get(orderIds, 'length', 0);
        const data = {
          FK_OrderNo: orderIds.join(','),
          ConsolNo: consolNo.toString(),
          CreatedAt: moment.tz('America/Chicago').format(),
          LastUpdatedAt: moment.tz('America/Chicago').format(),
          OrderIdCount: orderIdCount,
          Status: STATUSES.PENDING,
          ShipmentAparData: shipmentAparData,
        };
        console.info('🙂 -> file: index.js:43 -> data:', data);
        // If all conditions are met, store the data in the consol-status table
        await storeData(data);
      }
    });

    await Promise.all(promises);
  } catch (error) {
    console.error('Error processing records:', error);
  }
};

async function storeData(data) {
  const params = {
    TableName: CONSOL_STATUS_TABLE,
    Item: data,
  };

  try {
    await dynamodb.put(params).promise();
    console.info(`Data stored successfully:, ${JSON.stringify(data)}`);
  } catch (error) {
    console.error('Error storing data:', error);
  }
}

async function makeGetRequest(consolno) {
  const apiKey = 'ymSMkcdTJ5E7A4rP6em02qga3anLajSRCTpuf610';
  const url = `https://6enkvvfa45.execute-api.us-east-1.amazonaws.com/dev/consol/details?consolno=${consolno}`;

  try {
    const response = await axios.get(url, {
      headers: {
        'x-api-key': apiKey,
      },
    });

    console.info(JSON.stringify(response.data));
    return response.data;
  } catch (error) {
    console.error(error);
    throw error;
  }
}
