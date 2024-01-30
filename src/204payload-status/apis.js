'use strict';
const axios = require('axios');
const _ = require('lodash');

const { AUTH, GET_LOC_URL, CREATE_LOC_URL, SEND_PAYLOAD_URL, UPDATE_PAYLOAD_URL } = process.env;

async function getLocationId(name, address1, address2, cityName, state, zipCode) {
  const apiUrl = `${GET_LOC_URL}?name=${name}&address1=${address1}&address2=${
    address2 ?? ''
  }&city_name=${cityName}&state=${state}&zip_code=${zipCode}`;

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.get(apiUrl, {
      // params: queryParams,
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:30 -> getLocationId -> responseData:', responseData);
    // Return the location ID or perform additional processing as needed
    return _.get(responseData, '[0].id', false);
  } catch (error) {
    console.error('🙂 -> file: apis.js:34 -> getLocationId -> error:', error);
    return false;
  }
}

async function createLocation(data) {
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
    throw new Error(errorMessage);
  }
}

async function sendPayload({ payload: data }) {
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
    throw new Error(errorMessage);
  }
}

async function updateOrders({ payload: data }) {
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
    return _.get(responseData, 'id', false);
  } catch (error) {
    console.error('🙂 -> file: apis.js:58 -> updateOrders -> error:', error);
    throw error;
  }
}

module.exports = { getLocationId, createLocation, sendPayload, updateOrders };
