'use strict';
const axios = require('axios');
const _ = require('lodash');

const { AUTH, GET_LOC_URL, CREATE_LOC_URL } = process.env;

async function getLocationId(name, address1, address2, cityName, state, zipCode) {
  const apiUrl = GET_LOC_URL;
  const queryParams = {
    name,
    address1,
    address2,
    cityName,
    state,
    zipCode,
  };

  const headers = {
    Accept: 'application/json',
    Authorization: AUTH,
  };

  try {
    const response = await axios.get(apiUrl, {
      params: queryParams,
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:30 -> getLocationId -> responseData:', responseData);
    // Return the location ID or perform additional processing as needed
    return _.get(responseData, 'id', false);
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
    const response = await axios.post(apiUrl, data, {
      headers,
    });

    // Handle the response using lodash or other methods as needed
    const responseData = _.get(response, 'data', {});
    console.info('🙂 -> file: apis.js:54 -> createLocation -> responseData:', responseData);
    // Return the created location data or perform additional processing as needed
    return _.get(responseData, 'id', false);
  } catch (error) {
    console.error('🙂 -> file: apis.js:58 -> createLocation -> error:', error);
    throw error;
  }
}

module.exports = { getLocationId, createLocation };
