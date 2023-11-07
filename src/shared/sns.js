const AWS = require("aws-sdk");

async function snsPublishMessage(TopicArn, subject, message) {
    let snsParams;
    try {
        const sns = new AWS.SNS({ apiVersion: "2010-03-31" });
        snsParams = {
            TopicArn: TopicArn,
            Subject: subject,
            Message: `${message}.`,
        };
        await sns.publish(snsParams).promise();
    } catch (e) {
        console.error(
            "Sns publish message error: ",
            e,
            "\nparams: ",
            JSON.stringify(snsParams)
        );
        throw e;
    }
}

module.exports = { snsPublishMessage };
