<script runat="server">
    Platform.Load("Core", "1");

    var host = "https://test.salesforce.com/services"; // This is only for sandbox orgs
    //For prod/dev edition use https://login.salesforce.com/services

    var clientid, clientsecret, username, password;
    var prox = new Script.Util.WSProxy();
    var mid = XXXXXXXX; //BU MID goes here

    getSFConnectedAppCredentials(mid);

    if (clientid && clientsecret && username && password) {
        var tokenResponse = retrieveToken(
            host,
            clientid,
            clientsecret,
            username,
            password
        );

        var token = tokenResponse.access_token;
        var instURL = tokenResponse.instance_url;

        var bulkJobJSON = createBulkReq(instURL, token);

        var jobid = createBulkRequest.id;

        processBatchData(instURL, token, jobid, mid);

        var bulkJobCloseJSON = closeBulkReq(instURL, token, jobid);
    }

    /**
     * @function getSFConnectedAppCredentials
     * @description retrieves the credentials of the connected app of sales cloud which is stored in the DE
     * @param {String} mid - MID of the BU where the DE is stored
     */
    function getSFConnectedAppCredentials(mid) {
        if (mid) {
            prox.setClientId({ ID: mid }); //Impersonates the BU
        }

        //External key of DE with SF Credentials
        var ConnectedAppCredDECustKey = "XXXX-XXXX-XXXXX-XXXXXX";
        var cols = [
            "ConnectionName",
            "ClientId",
            "ClientSecret",
            "Username",
            "Password"
        ];
        var ConnectedAppCredDEReturn = prox.retrieve(
            "DataExtensionObject[" + ConnectedAppCredDECustKey + "]",
            cols
        );
        var ConnectedAppCredDEResults = ConnectedAppCredDEReturn.Results;
        var credRetrieved = false;

        for (var i = 0; i < ConnectedAppCredDEResults.length; i++) {
            if (credRetrieved) break;
            var ConnectedAppCredDERecord = ConnectedAppCredDEResults[i];
            for (
                var j = 0;
                j < ConnectedAppCredDERecord.Properties.length;
                j++
            ) {
                var name = ConnectedAppCredDERecord.Properties[j].Name;
                var value = ConnectedAppCredDERecord.Properties[j].Value;

                if (
                    name == "ConnectionName" &&
                    value == "SF CRM Connected App"
                ) {
                    credRtvd = true;
                }
                if (name == "ClientId") {
                    clientid = value;
                }
                if (name == "ClientSecret") {
                    clientsecret = value;
                }
                if (name == "Username") {
                    username = value;
                }
                if (name == "Password") {
                    password = value;
                }
            }
        }
    }

    /**
     * @function retrieveToken
     * @description retrieves the authentication token from the Connected App in SalesCLoud
     * @param {String} host - host URL of Sales cloud org
     * @param {String} clientid - ClientID of Sales cloud org
     * @param {String} clientsecret - Client Secret of Sales cloud org
     * @param {String} username - username for login into the Sales cloud org
     * @param {String} password - password of Sales cloud org also make sure to append the security token of your account behind your password while storing in the DE
     */

    function retrieveToken(host, clientid, clientsecret, username, password) {
        var tokenstr =
            "/oauth2/token?grant_type=password&client_id=" +
            clientid +
            "&client_secret=" +
            clientsecret +
            "&username=" +
            username +
            "&password=" +
            password;
        var url = host + tokenstr;
        var req = new Script.Util.HttpRequest(url);
        req.emptyContentHandling = 0;
        req.retries = 2;
        req.continueOnError = true;
        req.contentType = "application/json";
        req.method = "POST";

        var resp = req.send();
        var resultStr = String(resp.content);
        var resultJSON = Platform.Function.ParseJSON(String(resp.content));

        return resultJSON;
    }

    /**
     * @function createBulkRequest
     * @description creates the bulk request
     * @param {String} host - host URL of Sales cloud org
     * @param {String} token - token retrieved from Sales cloud via the retrieveToken() function
     */

    function createBulkRequest(host, token) {
        var url = host + "/services/data/v49.0/jobs/ingest/";
        var payload = {};
        payload.object = "Subscriber__c";
        payload.contentType = "CSV";
        payload.operation = "insert";
        payload.lineEnding = "CRLF";

        var req = new Script.Util.HttpRequest(url);
        req.emptyContentHandling = 0;
        req.retries = 2;
        req.continueOnError = true;
        req.contentType = "application/json";
        req.method = "POST";
        req.setHeader("Authorization", "Bearer " + token);
        req.postData = JSON.Stringify(payload);

        var resp = req.send();
        var resultStr = String(resp.content);
        var resultJSON = Platform.Function.ParseJSON(String(resp.content));

        return resultJSON;
    }

    /**
     * @function processBatchData
     * @description processes the batch data from the DE and adds it to a csv format
     * @param {String} instanceURL - host URL of Sales cloud org
     * @param {String} token - token retrieved from Sales cloud via the retrieveToken() function
     */

    function processBatchData(instanceURL, token, jobid, mid) {
        //Provide External key for SFSubscribersDE
        var deCustKey = "XXXX-XXXX-XXXX-XXXX";
        //SFSubscribersDE Fields
        var cols = ["Columns of the DE"];
        var moreData = true; //To validate if more data in Retrieve
        var reqID = null; //Used with Batch Retrieve to get more data
        var batchCount = 0;
        //String to store CSV Data to send to SF Bulk API
        var csvData =
            "comma separated value of your DE attribute names" + "\r\n"; // this is required for the carriage return line feed and also make sure to exclued _customerkey per batch

        while (moreData) {
            batchCount++;
            moreData = false;
            //Call function to get records from DE
            var deReturn = getDERowsIntoArray(mid, deCustKey, cols, reqID);

            moreData = deReturn.HasMoreRows;
            reqID = deReturn.RequestID;

            //iterate for each batch of 2500 records returned
            for (var i = 0; i < deReturn.Results.length; i++) {
                var recArray = [];
                var currRecord = deReturn.Results[i];
                for (var j = 0; j < currRecord.Properties.length; j++) {
                    if (currRecord.Properties[j].Name != "_CustomObjectKey")
                        recArray.push(currRecord.Properties[j].Value);
                }
                csvData += recArray.join(",") + "\r\n";
            }
            //Use batchCount if needed for debug log to identify number of batches called;
        }
        //Send update request to Bulk API job with final CSV Data
        var updJobJSON = updateBulkReq(instanceURL, token, jobid, csvData);
    }

    function getDERowsIntoArray(mid, deCustKey, cols, reqID) {
        if (mid) {
            prox.setClientId({ ID: mid }); //Impersonates the BU
        }

        if (reqID == null) {
            var deRecs = prox.retrieve(
                "DataExtensionObject[" + deCustKey + "]",
                cols
            ); //executes the proxy call
        } else {
            deRecs = prox.getNextBatch(
                "DataExtensionObject[" + deCustKey + "]",
                reqID
            );
        }

        return deRecs;
    }

    /**
     * @function updateBulkReq
     * @description uploads the csv data into the bulk job
     * @param {String} host - host URL of Sales cloud org
     * @param {String} token - token retrieved from Sales cloud via the retrieveToken() function
     * @param {String} jobid - jobid of the bulk job
     * @param {Array} csvData - Array of the CSV data
     */

    function updateBulkReq(host, token, jobid, csvData) {
        var url =
            host + "/services/data/v49.0/jobs/ingest/" + jobid + "/batches";
        var req = new Script.Util.HttpRequest(url);
        req.emptyContentHandling = 0;
        req.retries = 2;
        req.continueOnError = true;
        req.contentType = "text/csv";
        req.method = "PUT";
        req.setHeader("Authorization", "Bearer " + token);
        req.postData = csvData;

        var resp = req.send();
        var resultStr = String(resp.content);
        var resultJSON = Platform.Function.ParseJSON(String(resp.content));

        return resultJSON;
    }

    
    /**
     * @function closeBulkReq
     * @description closes the bulk job
     * @param {String} host - host URL of Sales cloud org
     * @param {String} token - token retrieved from Sales cloud via the retrieveToken() function
     * @param {String} jobid - jobid of the bulk job
     */

    function closeBulkReq(host, token, jobid) {
        var url = host + "/services/data/v49.0/jobs/ingest/" + jobid;
        var payload = {};
        payload.state = "UploadComplete";

        var req = new Script.Util.HttpRequest(url);
        req.emptyContentHandling = 0;
        req.retries = 2;
        req.continueOnError = true;
        req.contentType = "application/json";
        req.method = "PATCH";
        req.setHeader("Authorization", "Bearer " + token);
        req.postData = JSON.Stringify(payload);

        var resp = req.send();
        var resultStr = String(resp.content);
        var resultJSON = Platform.Function.ParseJSON(String(resp.content));

        return resultJSON;
    }
</script>
