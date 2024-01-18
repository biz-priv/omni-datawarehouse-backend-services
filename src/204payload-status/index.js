const AWS = require("aws-sdk");
const { prepareBatchFailureObj } = require("../shared/dataHelper");
const { loadP2PNonConsol } = require("../shared/loadP2PNonConsol");
const { loadP2PConsole } = require("../shared/loadP2PConsole");
const { loadMultistopConsole } = require("../shared/loadMultistopConsole");
const axios = require('axios');
const _ = require('lodash');

module.exports.handler = async (event, context, callback) => {
  const dynamoEventRecords = event.Records;

  try {
    const promises = dynamoEventRecords.map(async (record) => {
      try {

        const shipmentAparData = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        const dynamoData = _.get(record, "dynamodb", "")

        if (_.get(shipmentAparData, 'ConsolNo') === "0") {
          await loadP2PNonConsol(dynamoData, shipmentAparData);
        } else if (
          _.parseInt(_.get(shipmentAparData, 'ConsolNo', 0)) > 0 &&
          _.parseInt(_.get(shipmentAparData, 'SeqNo', 0)) < 9999
        ) {
          if (_.includes(['HS', 'TL'], _.get(shipmentAparData, 'FK_ServiceId')) && _.get(shipmentAparData, 'Consolidation') === 'Y') {
            await loadP2PConsole(dynamoData, shipmentAparData);
          } else if (_.get(shipmentAparData, 'FK_ServiceId') === 'MT') {
            await loadMultistopConsole(dynamoData, shipmentAparData);
          } else {
            console.log('Exception consol> 0 and shipmentApar.FK_ServiceId is not in HS/TL/MT');
            throw new Error('Exception');
          }
        } else {
          console.log('Exception global');
          throw new Error('Exception');
        }
      } catch (error) {
        console.log('Error', error);
      }
    });

    await Promise.all(promises);

    return prepareBatchFailureObj([]); // Modify if needed
  } catch (error) {
    console.error('Error', error);
    return prepareBatchFailureObj(dynamoEventRecords); // Modify if needed
  }
};





async function callLvlpApi(name, address1, city_name, state, zip_code) {
  const apiUrl = 'https://tms-lvlp.loadtracking.com:6790/ws/api/locations/search';
  const queryParams = {
    name,
    address1,
    city_name,
    state,
    zip_code
  };

  const headers = {
    Accept: 'application/json',
    Authorization: 'Basic YXBpdXNlcjpsdmxwYXBpdXNlcg=='
  };

  try {
    const response = await axios.get(apiUrl, {
      params: queryParams,
      headers: headers
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.log('API Response:', responseData);

    // Return the data or perform additional processing as needed
    return responseData;
  } catch (error) {
    // Handle errors
    console.error('Error calling API:', error.message);
    throw error;
  }
}