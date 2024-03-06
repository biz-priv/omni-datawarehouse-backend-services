'use strict';

const { get } = require('lodash');

module.exports.handler = async (event) => {
  await Promise.all(
    event.Records.map((record) => {
      const body = JSON.parse(get(record, 'body', ''));
      const message = JSON.parse(get(body, 'Message', ''));
      console.info('🙂 -> file: index.js:9 -> module.exports.handler= -> message:', typeof message);
      const newImage = get(message, 'NewImage', {});
      console.info('🙂 -> file: index.js:11 -> module.exports.handler= -> newImage:', newImage);
      const oldImage = get(message, 'OldImage', {});
      console.info('🙂 -> file: index.js:13 -> module.exports.handler= -> oldImage:', oldImage);
      const dynamoTableName = get(message, 'dynamoTableName', '');
      console.info(
        '🙂 -> file: index.js:15 -> module.exports.handler= -> dynamoTableName:',
        dynamoTableName
      );
      return true;
    })
  );
};
