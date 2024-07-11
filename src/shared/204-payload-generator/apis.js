'use strict';
const axios = require('axios');
const _ = require('lodash');
const xml2js = require('xml2js');

const {
  AUTH,
  GET_LOC_URL,
  CREATE_LOC_URL,
  SEND_PAYLOAD_URL,
  UPDATE_PAYLOAD_URL,
  TRACKING_NOTES_API_URL,
  API_PASS,
  API_USER_ID,
  GET_ORDERS_API_ENDPOINT,
  GET_CUSTOMER_URL,
} = process.env;

async function getLocationId(name, address1, address2, state) {
  const handleSpecialCharacters = (inputString) => {
    if (/[^a-zA-Z0-9 ]/.test(inputString)) {
      let outputString = inputString.replace(/[^a-zA-Z0-9 ]/g, '*');
      const starPosition = outputString.indexOf('*');
      if (starPosition === 0) {
        outputString = '*';
      }
      if (starPosition !== -1) {
        outputString = outputString.substring(0, starPosition + 1);
      }
      outputString = outputString.replace(' *', '*');
      return outputString;
    }
    return inputString;
  };

  // Create local variables to store modified values
  const modifiedName = handleSpecialCharacters(name);
  console.info('🚀 ~ file: test.js:1182 ~ getLocationId ~ modifiedName:', modifiedName);
  const modifiedAddress1 = handleSpecialCharacters(address1);
  console.info('🚀 ~ file: test.js:1184 ~ getLocationId ~ modifiedAddress1:', modifiedAddress1);
  const modifiedAddress2 = handleSpecialCharacters(address2);
  console.info('🚀 ~ file: test.js:1186 ~ getLocationId ~ modifiedAddress2:', modifiedAddress2);

  const combinations = [
    { name: modifiedName, address1: modifiedAddress1, address2: modifiedAddress2, state },
    { name: modifiedName, address1: modifiedAddress1, state },
    { name: modifiedName, address1: modifiedAddress1 },
    { name: modifiedName },
    { address1: modifiedAddress1 },
  ];

  const apiUrlBase = `${GET_LOC_URL}`;
  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  for (const combination of combinations) {
    const queryParams = new URLSearchParams(combination);
    const apiUrl = `${apiUrlBase}?${queryParams}`;

    console.info('🚀 ~ file: apis.js:62 ~ getLocationId ~ apiUrl:', apiUrl);
    try {
      const response = await axios.get(apiUrl, { headers });
      const responseData = _.get(response, 'data', []);

      // Add a check to ensure responseData is not null before filtering
      if (responseData !== null && responseData.length > 0) {
        // Remove asterisks from modifiedName, modifiedAddress1, and modifiedAddress2
        const cleanedName = modifiedName.replace('*', '');
        const cleanedAddress1 = modifiedAddress1.replace('*', '');

        // Filter response data to ensure all fields match the provided parameters
        const filteredData = responseData.filter(
          (item) =>
            (_.toUpper(removePunctuation(cleanedName)) ===
              _.toUpper(removePunctuation(item.name)) ||
              _.startsWith(
                _.toUpper(removePunctuation(item.name)),
                _.toUpper(removePunctuation(cleanedName))
              )) &&
            (_.toUpper(removePunctuation(cleanedAddress1)) ===
              _.toUpper(removePunctuation(item.address1)) ||
              _.startsWith(
                _.toUpper(removePunctuation(item.address1)),
                _.toUpper(removePunctuation(cleanedAddress1))
              )) &&
            _.toUpper(removePunctuation(item.state)) === _.toUpper(removePunctuation(state))
        );

        if (!_.isEmpty(filteredData)) {
          console.info('🙂 -> filteredData[0]:', filteredData[0]);
          console.info('🙂 -> filteredData[0].id:', filteredData[0].id);
          return _.get(filteredData, '[0].id', false);
        }
      } else {
        console.error('🙂 -> file: apis.js:34 -> getLocationId -> responseData is null');
      }
    } catch (error) {
      console.error('🙂 -> file: apis.js:34 -> getLocationId -> error:', error);
      return false;
    }
  }
  return false;
}

function removePunctuation(str) {
  return str.replace(/[^\w\s]/g, '');
}

async function getOrders({ id }) {
  const apiUrl = `${GET_ORDERS_API_ENDPOINT}/${id}`;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.get(apiUrl, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:30 -> getOrders -> responseData:', responseData);
    // Return the location ID or perform additional processing as needed
    return responseData;
  } catch (error) {
    console.error('🙂 -> file: apis.js:34 -> getOrders -> error:', error);
    return false;
  }
}

async function createLocation({ data, orderId, consolNo = 0, country, houseBill }) {
  if (country.toLowerCase() === 'us' && _.get(data, 'zip_code', 0).length > 5) {
    data.zip_code = data.zip_code.slice(0, 5);
  }
  const apiUrl = CREATE_LOC_URL;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.put(apiUrl, data, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:54 -> createLocation -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return _.get(responseData, 'id', false);
  } catch (error) {
    const errorMessage = _.get(error, 'response.data', error.message);
    console.error('🙂 -> file: apis.js:58 -> createLocation -> error:', error);
    throw new Error(`\nError in Create Location API.
    \n FK_OrderNo: ${orderId}
    \n ConsolNo: ${consolNo}
    \n Housebill: ${houseBill}
    \n Error Details: ${errorMessage}
    \n Payload:
    \n ${JSON.stringify(data)}`);
  }
}

async function sendPayload({ payload: data, orderId, consolNo = 0, houseBillString }) {
  const apiUrl = SEND_PAYLOAD_URL;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.put(apiUrl, data, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:74 -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return responseData;
  } catch (error) {
    console.info('🙂 -> file: apis.js:78 -> error:', error);
    const errorMessage = _.get(error, 'response.data', error.message);
    throw new Error(`\nError in Create Orders API.
    \n FK_OrderNo: ${orderId}
    \n ConsolNo: ${consolNo}
    \n Housebill: ${houseBillString}
    \n Error Details: ${errorMessage}
    \n Payload: 
    \n ${JSON.stringify(data)}`);
  }
}

async function updateOrders({ payload: data, orderId, consolNo = 0, houseBillString }) {
  const apiUrl = UPDATE_PAYLOAD_URL;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.put(apiUrl, data, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:54 -> updateOrders -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return responseData;
  } catch (error) {
    console.info('🙂 -> file: apis.js:103 -> error:', error);
    const errorMessage = _.get(error, 'response.data', error.message);
    throw new Error(`\nError in Update Orders API.
    \n FK_OrderNo: ${orderId}
    \n ConsolNo: ${consolNo}
    \n Housebill: ${houseBillString}
    \n Error Details: ${errorMessage}
    \n Payload:
    \n ${JSON.stringify(data)}`);
  }
}

/**
 * update WorlTrack XML api
 * @param {*} houseBill
 * @param {*} shipmentId
 * @returns
 */

async function liveSendUpdate(houseBill, shipmentId) {
  const requestBody = `<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Header>
        <AuthHeader xmlns="http://tempuri.org/">
          <UserName>${API_USER_ID}</UserName>
          <Password>${API_PASS}</Password>
        </AuthHeader>
      </soap:Header>
      <soap:Body>
        <WriteTrackingNote xmlns="http://tempuri.org/">
          <HandlingStation></HandlingStation>
          <HouseBill>${houseBill}</HouseBill>
          <TrackingNotes>
            <TrackingNotes>
              <TrackingNoteMessage>LiVe Logistics shipment number ${shipmentId}</TrackingNoteMessage>
            </TrackingNotes>
          </TrackingNotes>
        </WriteTrackingNote>
      </soap:Body>
    </soap:Envelope>`;

  try {
    const response = await axios.post(TRACKING_NOTES_API_URL, requestBody, {
      headers: {
        'Content-Type': 'text/xml',
        'Accept': 'text/xml',
      },
    });

    const xmlParser = new xml2js.Parser({ explicitArray: false });
    const parsedResponse = await xmlParser.parseStringPromise(response.data);

    const result =
      parsedResponse['soap:Envelope']['soap:Body'].WriteTrackingNoteResponse
        .WriteTrackingNoteResult;

    if (result === 'Success') {
      console.info(`updated in tracking notes API with shipmentId: ${shipmentId}`);
      return parsedResponse;
    }
    parsedResponse['soap:Envelope']['soap:Body'].WriteTrackingNoteResponse.WriteTrackingNoteResult =
      'Failed';
    return parsedResponse;
  } catch (error) {
    console.error('Error in liveSendUpdate:', error);
    return error.response ? error.response.data : 'liveSendUpdate error';
  }
}

async function getCustomerData({ customerId, orderId, consolNo = 0, houseBillString }) {
  const apiUrl = GET_CUSTOMER_URL;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    if (!customerId) throw new Error('Customer ID not found.');
    const response = await axios.get(`${apiUrl}/${customerId}`, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:74 -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return responseData;
  } catch (error) {
    console.info('🙂 -> file: apis.js:78 -> error:', error);
    const errorMessage = _.get(error, 'response.data', error.message);
    throw new Error(`\nError in Create Orders API.
    \n FK_OrderNo: ${orderId}
    \n ConsolNo: ${consolNo}
    \n Housebill: ${houseBillString}
    \n Error Details: ${errorMessage}
    \n CustomerId: ${customerId}`);
  }
}

module.exports = {
  getLocationId,
  createLocation,
  sendPayload,
  updateOrders,
  liveSendUpdate,
  getOrders,
  getCustomerData,
};
