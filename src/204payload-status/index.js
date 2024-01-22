'use strict';
const AWS = require('aws-sdk');
const { prepareBatchFailureObj } = require('../shared/dataHelper');
const _ = require('lodash');
const { nonConsolPayload, consolPayload } = require('./payloads');
const {
  fetchLocationId,
  getFinalShipperAndConsigneeData,
  fetchAparTableForConsole,
  fetchCommonTableData,
  fetchNonConsoleTableData,
  fetchConsoleTableData,
} = require('./helper');

module.exports.handler = async (event) => {
  console.info(
    '🙂 -> file: index.js:8 -> module.exports.handler= -> event:',
    JSON.stringify(event)
  );
  const dynamoEventRecords = event.Records;

  try {
    const promises = dynamoEventRecords.map(async (record) => {
      try {
        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

        const { confirmationCostData, shipperData, consigneeData } = await fetchCommonTableData({
          shipmentAparData,
        });

        if (!consigneeData || !shipperData) {
          console.error('Shipper or Consignee tables are not populated.');
          throw new Error('Shipper or Consignee tables are not populated.');
        }

        const { finalConsigneeData, finalShipperData } = getFinalShipperAndConsigneeData({
          confirmationCostData,
          consigneeData,
          shipperData,
        });

        const { shipperLocationId, consigneeLocationId } = await fetchLocationId({
          finalConsigneeData,
          finalShipperData,
        });

        console.info(
          '🙂 -> file: index.js:83 -> promises ->  shipperLocationId, consigneeLocationId:',
          shipperLocationId,
          consigneeLocationId
        );

        if (!shipperLocationId || !consigneeLocationId) {
          console.error('Could not fetch location id.');
          throw new Error('Could not fetch location id.');
        }

        // Non-Console
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', 0), 10) === 0 &&
          _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          const { shipmentHeaderData, referencesData, shipmentDescData, customersData } =
            await fetchNonConsoleTableData({ shipmentAparData });
          if (!shipmentHeaderData) {
            return 'Shipment header data is missing.';
          }
          const nonConsolPayloadData = await nonConsolPayload({
            referencesData,
            customersData,
            consigneeLocationId,
            finalConsigneeData,
            finalShipperData,
            shipmentDesc: shipmentDescData,
            shipmentHeader: shipmentHeaderData,
            shipperLocationId,
          });
          console.info(
            '🙂 -> file: index.js:114 -> nonConsolPayloadData:',
            JSON.stringify(nonConsolPayloadData)
          );
          return nonConsolPayloadData;
        }

        // Console
        if (
          parseInt(_.get(shipmentAparData, 'ConsolNo', 0), 10) > 0 &&
          _.get(shipmentAparData, 'Consolidation') === 'Y' &&
          _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId'))
        ) {
          const shipmentAparDataForConsole = await fetchAparTableForConsole({
            orderNo: _.get(shipmentAparData, 'FK_OrderNo'),
          });
          console.info(
            '🙂 -> file: index.js:121 -> shipmentAparDataForConsole:',
            shipmentAparDataForConsole
          );
          const {
            shipmentHeaderData,
            referencesData,
            shipmentDescData,
            trackingNotesData,
            customersData,
            userData,
          } = await fetchConsoleTableData();
          const nonConsolPayloadData = await consolPayload({
            referencesData,
            customersData,
            consigneeLocationId,
            finalConsigneeData,
            finalShipperData,
            shipmentDesc: shipmentDescData,
            shipmentHeader: shipmentHeaderData,
            shipperLocationId,
            shipmentAparData,
            trackingNotesData,
            userData,
          });
          console.info(
            '🙂 -> file: index.js:114 -> nonConsolPayloadData:',
            JSON.stringify(nonConsolPayloadData)
          );
          return 'Console payload';
        }
        // else if (
        //   _.parseInt(_.get(shipmentAparData, 'ConsolNo', 0)) > 0 &&
        //   _.parseInt(_.get(shipmentAparData, 'SeqNo', 0)) < 9999
        // ) {
        //   if (
        //     _.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId')) &&
        //     _.get(shipmentAparData, 'Consolidation') === 'Y'
        //   ) {
        //     await loadP2PConsole(dynamoData, shipmentAparData);
        //   } else if (_.get(shipmentAparData, 'FK_ServiceId') === 'MT') {
        //     return
        //     // await loadMultistopConsole(dynamoData, shipmentAparData);
        //   } else {
        //     console.info('Exception consol> 0 and shipmentApar.FK_ServiceId is not in HS/TL/MT');
        //     throw new Error('Exception');
        //   }
        // } else {
        //   console.info('Exception global');
        //   throw new Error('Exception');
        // }
      } catch (error) {
        console.info('Error', error);
      }
      return false;
    });

    await Promise.all(promises);

    return prepareBatchFailureObj([]); // Modify if needed
  } catch (error) {
    console.error('Error', error);
    return prepareBatchFailureObj(dynamoEventRecords); // Modify if needed
  }
};
