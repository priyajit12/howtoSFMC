<script runat="server">
    Platform.Load('core', '1')

    /* -------------------------------------------------------------------------

        1. Global Variables
            1.1. Contact Deletion DE Key
            1.2. REST API Authorization
            1.3. Error Handling
        2. Helper Functions
            2.1. Debugging
            2.2. Error handling
        3. Main Script
            3.1. REST API Authorization
            3.2. Contact Deletion Process

    -------------------------------------------------------------------------- */

    /* ----------------------------------------------------------------------- */
    /* ------------------------- 1. GLOBAL VARIABLES ------------------------- */
    /* ----------------------------------------------------------------------- */

    /* ------------------------ 1.1 Contact Deletion DE Key -------------------------- */

    var contactDeletionDEKey = 'contact-deletion-pending-de';

    /* ------------------------ 1.2 REST API Authorization --------------------------- */

    var payload, endpoint, response;

    var clientSecret = 'clientSecretFromInstalledPackage';
    var clientID = 'clientIDFromInstalledPackage';
    var clientBase = 'clientBaseUrl'
    var contentType = 'application/json';
    var debugging = false;


    /* ------------------------ 1.3 Error Handling --------------------------- */

    var errorDE = 'error-log-de';
    var automationName = 'contact-deletion-process';


    /* ----------------------------------------------------------------------- */
    /* ------------------------- 2. HELPER FUNCTIONS ------------------------- */
    /* ----------------------------------------------------------------------- */

    /* --------------------------- 2.1. Debugging ---------------------------- */

    /**
     * @function debugValue
     * @description Outputs provided description and SSJS value to front-end in a type-safe & consistent way
     * @param {string} description - Describes meaning of the second parameter in the output
     * @param {*} value - The value that needs to be debugged
     */
    function debugValue(description, value) {
        Write(description + ': ' + (typeof value == 'object' ? Stringify(value) : value) + '<br><br>');
    };

    /* -------------------------- 2.2. Error handling ------------------------ */

    /**
     * @function handleError
     * @description Adds the error with context to error logging Data Extension and redirects to an error page.
     * @param {Object} error - The caught error object. It can come from the try/catch block or be manually created.
     * @param {string} error.message - First error key stores a short error message describing the issue.
     * @param {string} error.description - Second error key stores detailed error path helping with root cause analysis
     */
    function handleError(error) {
        if (debugging) {
            debugValue('Found error', error);
        } else {
            Platform.Function.InsertData(errorDE, ['ID', 'ErrorSource', 'ErrorMessage', 'ErrorDescription'], [GUID(), automationName, error.message, error.description]);
        };
    };


    /* ----------------------------------------------------------------------- */
    /* --------------------------- 3. MAIN SCRIPT ---------------------------- */
    /* ----------------------------------------------------------------------- */

    /* ------------------- 3.1. REST API Authorization --------------------- */

    endpoint = 'https://' + clientBase + '.auth.marketingcloudapis.com/v2/token';
    payload = {
        client_id: clientID,
        client_secret: clientSecret,
        grant_type: 'client_credentials'
    };
    if (debugging) debugValue('Payload', payload);

    try {
        response = HTTP.Post(endpoint, contentType, Stringify(payload));
    } catch (error) {
        handleError(error);
    }
    var accessToken = Platform.Function.ParseJSON(response['Response'][0]).access_token;
    if (debugging) debugValue('AccessToken', accessToken);

    /* ------------------- 3.2. Contact Deletion Process --------------------- */

    endpoint = 'https://' + clientBase + '.rest.marketingcloudapis.com/contacts/v1/contacts/actions/delete?type=listReference';
    payload = {
        deleteOperationType: 'ContactAndAttributes',
        targetList: {
            listType: { listTypeID: 3 },
            listKey: contactDeletionDEKey
        },
        deleteListWhenCompleted: false,
        deleteListContentsWhenCompleted: true
    };
    var headerNames = ['Authorization'];
    var headerValues = ['Bearer ' + accessToken];

    try {
        response = HTTP.Post(endpoint, contentType, Stringify(payload), headerNames, headerValues);
        if (debugging) debugValue('Response', response);
    } catch (error) {
        handleError(error);
    }
</script>