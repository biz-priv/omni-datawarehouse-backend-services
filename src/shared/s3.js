const AWS = require('aws-sdk');
const { get } = require('lodash');
const s3 = new AWS.S3({ region: process.env.REGION });

/**
 * The function puts a JSON object into an AWS S3 bucket.
 * @param data - The data that needs to be uploaded to the S3 bucket. It should be in JSON format.
 * @param fileName - The name of the file that will be uploaded to the specified S3 bucket.
 * @param bucket - The name of the S3 bucket where the object will be stored.
 */
async function putObject(data, fileName, bucket, contentType) {
  try {
    const s3Params = {
      Bucket: bucket,
      Key: fileName,
      ContentType: contentType,
      Body: data,
    };

    await s3.putObject(s3Params).promise();
  } catch (e) {
    console.error('s3 put object error: ', e);
    throw e;
  }
}

/**
 * This function retrieves a JSON object from an AWS S3 bucket and returns it as a parsed object.
 * @param bucket - The name of the S3 bucket where the object is stored.
 * @param key - The key is a unique identifier for an object in an S3 bucket. It is used to retrieve
 * the object from the bucket.
 * @returns The `getObject` function is returning the parsed JSON data from an S3 object with the
 * specified `bucket` and `key`. If there is an error, it will log the error and throw an error with a
 * specific error code and correlation ID.
 */
async function getObject(bucket, key) {
    try {
        const params = {
            Bucket: bucket,
            Key: key,
            ResponseContentType: "application/json",
        };
        console.log(`🙂 -> file: s3.js:42 -> params:`, params);
        const s3File = await s3.getObject(params).promise();
        console.log(`🙂 -> file: s3.js:44 -> s3File:`, s3File);
        // return s3File
        return get(s3File, "Body", "").toString("utf-8").trim();
    } catch (e) {
        console.error("s3 get object error: ", e);
        throw e;
    }
}

/**
 * This function retrieves a Gzip object from an AWS S3 bucket and returns its body.
 * @param bucket - The name of the Amazon S3 bucket where the object is stored.
 * @param key - The key parameter is a string that represents the name of the object in the S3 bucket
 * that you want to retrieve.
 * @returns the body of a file retrieved from an S3 bucket, after unzipping it if it was compressed in
 * gzip format.
 */
async function getGzipObject(bucket, key) {
  try {
    const params = {
      Bucket: bucket,
      Key: key,
      ResponseContentType: 'application/json',
    };

    const s3File = await s3.getObject(params).promise();
    return s3File.Body;
  } catch (e) {
    console.error('s3 get object error: ', e);
    throw e;
  }
}

/**
 * This function retrieves a list of objects from an AWS S3 bucket.
 * @param bucket - The name of the S3 bucket from which to retrieve the list of objects.
 * @returns The function `getListObjects` returns a list of objects in the specified S3 bucket.
 * Specifically, it returns the `Contents` property of the `fileList` object returned by the
 * `listObjectsV2` method of the AWS SDK for JavaScript.
 */
async function getListObjects(bucket) {
  try {
    const fileList = await s3.listObjectsV2({ Bucket: bucket }).promise();
    return fileList.Contents;
  } catch (e) {
    console.error('s3 get list objects error: ', e);
    throw e;
  }
}

/**
 * The function moves an object from a source location to a destination location in an AWS S3 bucket.
 * @param bucket - The name of the Amazon S3 bucket where the object is located and where it will be
 * moved to.
 * @param source - The source parameter is the path of the object that needs to be moved in the S3
 * bucket. It should be in the format "bucket-name/object-key".
 * @param destination - The destination parameter is the key or path of the object in the S3 bucket
 * where the source object will be moved to.
 */
async function moveObject(bucket, source, destination) {
  try {
    const params = {
      Bucket: bucket,
      CopySource: source,
      Key: destination,
    };
    let sourceSplit = source.split('/');
    await s3.copyObject(params).promise();
    await s3
      .deleteObject({
        Bucket: bucket,
        Key: sourceSplit.slice(1, sourceSplit.length).join('/'),
      })
      .promise();
  } catch (e) {
    console.error('s3 move object error: ', e);
    throw e;
  }
}

async function deleteObject(bucket, key) {
  try {
    const params = {
      Bucket: bucket,
      Key: key,
    };
    await s3.deleteObject(params).promise();
  } catch (e) {
    console.error('s3 delete object error: ', e);
    throw e;
  }
}

async function checkIfObjectExists(bucket, key) {
    return new Promise((resolve, reject) => {
        const params = {
            Bucket: bucket,
            Key: key,
        };

        s3.headObject(params, (err, data) => {
            if (err) {
                if (err.code === "NotFound") {
                    resolve(false);
                } else {
                    console.error("Error checking object existence:", err);
                    reject(err);
                }
            } else {
                console.log("Object exists. Metadata:", data);
                resolve(true);
            }
        });
    });
}

module.exports = {
    putObject,
    getObject,
    getListObjects,
    moveObject,
    getGzipObject,
    deleteObject,
    checkIfObjectExists,
};
