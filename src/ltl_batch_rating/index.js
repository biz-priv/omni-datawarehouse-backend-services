module.exports.handler = (event, context) => {
    console.log(JSON.stringify(event, null, 2));
    if (new Date().getTime() % 2 === 0) {
        return { hasMoreData: "true" };
    } else {
        return { hasMoreData: "false" };
    }
};
