"""
EconomicFilters
-------------------------
Datastream provides access to nearly 16 million economic series. With coverage of this extent, it can be difficult to prioritise 
which region, country, sector or industry to analyse and investigate. With this in mind, clients that access the full Datatstream Web Service 
can now poll for the latest changes and corrections to any of the economic series.

Even with polling for changes and corrections, the large number of economic series supported can produce a large number of updates to process each day.
To reduce the number of updates, Datastream provides a global filter, DATASTREAM_KEYIND_GLOBAL, that comprises the 25K most prominent series. Querying
for updates using this filter can significantly reduce the number of updates reported.

Clients can also create their own custom filters comprising up to 100K series and use these to query for changes and corrections.

This module defines the EconomicFilters and ancillary classes that permit users to manage their custom filters.
"""

import re
import requests
import json
import pytz
from datetime import datetime, timedelta, date
from .DSUserDataObjectBase import *
import platform
import configparser
import time
from enum import IntEnum


class DSEconFilterJsonDateTimeEncoder(json.JSONEncoder):
    """ 
    DSEconFilterJsonDateTimeEncoder is used in the conversion of datetime objects into JSON format. It's passed into json.dumps()

    Example:
    jsonText = json.dumps(reqObject, cls = DSEconFilterJsonDateTimeEncoder)
    """
    def default(self, obj):
        if isinstance(obj, datetime) or isinstance(obj, date):
            return DSUserObjectDateFuncs.toJSONdate(obj)
        # else fall through to json default encoder
        return json.JSONEncoder.default(self, obj)


class DSFilterUpdateActions(IntEnum):
    """
    EconomicFilters supports an UpdateFilter method that allows you to modify the contents of an existing filter.
    DSFilterUpdateActions specifies the update action.

    Options:
        CreateFilter: Reserved for internal filter validation checks.
        AppendConstituents: Used to append a list of economic series to existing list of filter constituents. 
        ReplaceConstituents: Used to completely replace the constituents of a filter.
        RemoveConstituents: Used to remove a subset of economic series from the current list of filter constituents.
        UpdateDescription: Used to modify the description for the filter.
        UpdateSharedState: Used to specify whether the filter is private to your Datastream ID or shared with other Datastream IDs belonging to your parent ID.
    """
    CreateFilter = 0
    AppendConstituents = 1
    ReplaceConstituents = 2
    RemoveConstituents = 3
    UpdateDescription = 4
    UpdateSharedState = 5

class DSFilterGetAllAction(IntEnum):
    """
    EconomicFilters supports a GetAllFilters method that allows you to query for all the existing filters currently available.
    DSFilterGetAllAction specifies the retrieval options.

    Options:
        PersonalFilters: Use this flag to retrieve only filters created with your Datastream ID
        SharedFilters: Use this flag to retrieve both your personal filters plus any filters shared by other child IDs that share your parent Datastream ID. 
        DatastreamFilters: Use this flag to retrieve just the list Datastream global filters available to all clients.
        AllFilters:  Use this flag to retrieve all private, shared or Datastream global filters.
    """
    PersonalFilters = 0
    SharedFilters = 1
    DatastreamFilters = 2
    AllFilters = 3

class DSFilterResponseStatus(IntEnum):
    """
    All EconomicFilters methods to retrieve or modify filters return a respone object which includes a ResponseStatus property.
    The ResponseStatus property specifies success or failure for the request using a DSFilterResponseStatus value

    Response Values:
        FilterSuccess: The request succeeded and the response object's Filter property should contain the (updated) filter (except for DeleteFilter method).
        FilterPermissions: Users need to be specifically permissioned to create custom filters. This flag is set if you are not currently permissioned.
        FilterNotPresent: Returned if the requested ID does not exist.
        FilterFormatError: Returned if your request filter ID is not in the correct format, or if you try and modify a Datastrem global filter (ID begins DATASTREAM*).
        FilterSizeError: Returned if your call to CreateFilter or ModifyFilter contains a list with zero or in excess of the 100K constituents.
        FilterConstituentsError: Returned if your supplied filter constituent list (on CreateFilter) contains no valid economic series. The filter won't be created.
        FilterError:  The generic error flag. This will be set for any error not specified above. Examples are:
            Requested filter ID is not present
            You have reached the maximum permitted number of custom economic filters (maximum 50)
            Requested filter ID (on CreateFilter) already exists
    """
    FilterSuccess = 0
    FilterPermissions = 1
    FilterNotPresent = 2
    FilterFormatError = 3
    FilterSizeError = 4
    FilterConstituentsError = 5
    FilterError = 6



class DSEconomicsFilter:
    """
    DSEconomicsFilter is the base object for retrieval or creating/modifying a filter.

    Properties
    ----------
    FilterID: The filter identifier must be between 5 and 45 characters long and contain only alphanumeric or underscore characters. e.g. MyFilter.
    Description: A string describing the filter. Max 127 alphanumeric characters.
    Constituents: An array of strings, each defining an economic series. Economic series are between 7 and 9 characters in length and contain
                  only alphanumeric characters plus the following special characters $&.%#£,
                  Examples: USGDP...D, USGB10YR, JPEMPA&FP, LBUN%TOT, UKIMPBOPB, USUNRP%DQ
    ConstituentsCount: The number of constituent items.
    Created: a datetime representing when the filter was first created.
    LastModified: a datetime representing when the filter was last modified.
    OwnerId: The Datastream parent ID that owns the filter. This will always be your Datastream parent ID or None if the filter is a Datastream global filter.
    Shared: A bool set to True if the filter is shared with all children of your Datastream parent ID. False indicates only your Datastream ID can use the filter.
    """
    def __init__(self, jsonDict = None):
        self.FilterId = None
        self.Description = None
        self.Constituents = None
        self.ConstituentsCount = 0
        self.Created = datetime.utcnow()  # only valid when received as a response. On create or update this field is ignored
        self.LastModified = datetime.utcnow() # only valid when received as a response. On create or update this field is ignored
        self.OwnerId = None
        self.Shared = False
        if jsonDict: # upon a successful response from the API server jsonDict will be used to populate the DSEconomicsFilter object with the response data.
            self.FilterId = jsonDict['FilterId']
            self.Description = jsonDict['Description']
            self.Constituents = jsonDict['Constituents']
            self.ConstituentsCount = jsonDict['ConstituentsCount']
            self.Created = DSUserObjectDateFuncs.jsonDateTime_to_datetime(jsonDict['Created'])
            self.LastModified = DSUserObjectDateFuncs.jsonDateTime_to_datetime(jsonDict['LastModified'])
            self.OwnerId = jsonDict['OwnerId']
            self.Shared = jsonDict['Shared']

    def SetSafeUpdateParams(self):
        """ SetSafeUpdateParams: The following parameters are set only in response when we query for economic filters. 
        This method is called before Create or Update to ensure safe values set prior to JSON encoding"""
        self.Created = datetime.utcnow()  # only valid when received as a response. On create or update this field is ignored
        self.LastModified = datetime.utcnow() # only valid when received as a response. On create or update this field is ignored
        self.ConstituentsCount = len(self.Constituents) if isinstance(self.Constituents, list) else 0 # the server gets the true size by inspecting the Constituents property
        self.Owner = None   # only valid when received as a response. On create or update this field is ignored
        self.Shared = self.Shared if isinstance(self.Shared, bool) else False
        self.Description = self.Description if isinstance(self.Description, str) else None


class DSEconomicsFilterResponse:
    """
    DSEconomicsFilterResponse is the object returned for the GetFilter, CreateFilter, UpdateFilter and DeleteFilter methods.

    Properties
    ----------
    Filter: Upon a successful response, the Filter property will contain a valid DSEconomicsFilter object. For a successful DeleteFilter call the value will be None.
    ResponseStatus: This property will contain a DSFilterResponseStatus value. DSFilterResponseStatus.FilterSuccess represents a successful response.
    ErrorMessage: If ResponseStatus is not DSFilterResponseStatus.FilterSuccess this status string will provide a description of the error condition.
    ItemErrors: When calling CeateFilter or UpdateFilter (AppendConstituents or ReplaceConstituents. Any supplied constituents which have an invalid format,
                or do not exist in the supported dataset (~16M economic series), will be returned as an array in this property.
    Properties: Not currently used and will currently always return None.

    """
    def __init__(self, jsonDict = None):
        self.Filter = None
        self.ResponseStatus = DSFilterResponseStatus.FilterSuccess
        self.ErrorMessage = None
        self.ItemErrors = None
        self.Properties = None
        if jsonDict: # upon a successful response from the API server jsonDict will be used to populate the DSEconomicsFilterResponse object  with the response data.
            self.ResponseStatus = DSFilterResponseStatus(jsonDict['ResponseStatus'])
            self.ErrorMessage = jsonDict['ErrorMessage']
            self.ItemErrors = jsonDict['ItemErrors']
            if jsonDict['Filter']:
                self.Filter = DSEconomicsFilter(jsonDict['Filter'])


class DSEconomicsFilterGetAllResponse:
    """
    DSEconomicsFilterGetAllResponse is the object returned for the GetAllFilters request only.

    Properties
    ----------
    Filters: Upon a successful response, the Filters property will contain a collection of DSEconomicsFilter objects.
    FilterCount: The number of filters returned in the Filters property.
    ResponseStatus: This property will contain a DSFilterResponseStatus value. DSFilterResponseStatus.FilterSuccess represents a successful response.
    ErrorMessage: If ResponseStatus is not DSFilterResponseStatus.FilterSuccess this status string will provide a description of the error condition.
    Properties: Not currently used and will currently always return None.

    Note: The returned filters will not have any constituents returned in the Constituents property. The ConstituentsCount property will tell you
    how many constituents there are in the filter. However, you will need to query for the individual filter to retrieve the list of constituents.

    """
    def __init__(self, jsonDict = None):
        self.Filters = None
        self.FilterCount = 0
        self.ResponseStatus = DSFilterResponseStatus.FilterSuccess
        self.ErrorMessage = None
        self.Properties = None
        if jsonDict:
            self.ResponseStatus = DSFilterResponseStatus(jsonDict['ResponseStatus'])
            self.ErrorMessage = jsonDict['ErrorMessage']
            self.FilterCount = jsonDict['FilterCount']
            if jsonDict['Filters'] is not None:
                self.Filters = [DSEconomicsFilter(jsonFilter) for jsonFilter in jsonDict['Filters']] 


class DSEconomicUpdateFrequency(IntEnum):
    """
    The GetEconomicChanges method can return a collection of DSEconomicChangeCorrection objects, each defining an economic series that has been updated.
    DSEconomicUpdateFrequency is a property of DSEconomicChangeCorrection which specifies the update frequency of the series.

    The values should be self explanatory. Note: at the time of writing, just 55 series exist with a frequency of SemiAnnually. These are all in the 
    Algerian market and sourced from Banque d’ Algerie.
    """
    Daily = 0
    Weekly = 1
    Monthly = 2
    Quarterly = 3
    SemiAnnually = 4
    Annually = 5


class DSEconomicChangeCorrection:
    """
    DSEconomicChangeCorrection is the class defining each economic series returned in the Updates property of the DSEconomicChangesResponse item
    returned in GetEconomicChanges queries.

    Properties
    ----------
    Series: The Datastream mnemonic for the economic series that has updated. e.g. 'JPTFMFLBF', 'USGDP...D', 'UKXRUSD.'
    Frequency: A DSEconomicUpdateFrequency enum describing the update frequency of the series.
    Updated: The update time in UTC when the notification of the change was received by Datastream.
    """
    def __init__(self, jsonDict):
        self.Series = jsonDict['Series']
        self.Frequency = DSEconomicUpdateFrequency(jsonDict['Frequency'])
        self.Updated = DSUserObjectDateFuncs.jsonDateTime_to_datetime(jsonDict['Updated'])


class DSEconomicChangesResponse:
    """
    DSEconomicChangesResponse is the object returned for the GetEconomicChanges request only.

    Properties
    ----------
    NextSequenceId: Upon a successful response, the NextSequenceId property will identify the next sequence to request. If the UpdatesPending property
                    is True, it indicates there are more updates available to be retrieved using the value in this field to request the subsequent updates. 
                    If the UpdatesPending property is False, it indicates that there are currently no more updates available and the returned NextSequenceId
                    value identifies the ID that will be assigned to the next update when it becomes available. This should be used in a periodic query 
                    (ideally every 10 minutes or more) for any new updates.
    FilterId: This field simply returns the ID of the optional custom filter used in the query.
    UpdatesCount: If the GetEconomicChanges request succeeds in returning updates, this field will contain the number of updates in the current response.
    Updates: If the GetEconomicChanges request succeeds in returning updates, this field will contain the collection of DSEconomicChangeCorrection objects
             defining the update details for each economic series.
    UpdatesPending: A GetEconomicChanges response contains a maximum of 10K updates. If the query detected that there were more than 10K items that updated
                    later than the requested sequence ID, UpdatesPending will be set True to indicate a subsequent request for the later updates should be
                    made using the ID specified in the returned NextSequenceId field. If set False, there are currently no more updates pending and NextSequenceId
                    identifies the ID that will be assigned to the next update when it becomes available.
    PendingCount:   If UpdatesPending is set True, this field identifies how many more updates are pending retrieval.
    ResponseStatus: This property will contain a DSFilterResponseStatus value. DSFilterResponseStatus.FilterSuccess represents a successful response.
    ErrorMessage: If ResponseStatus is not DSFilterResponseStatus.FilterSuccess this status string will provide a description of the error condition.
    Properties: Not currently used and will currently always return None.
    """

    def __init__(self, jsonDict = None):
        self.NextSequenceId = 0
        self.FilterId = None
        self.UpdatesCount = 0
        self.Updates = None
        self.UpdatesPending = False
        self.PendingCount = 0
        self.ResponseStatus = DSFilterResponseStatus.FilterSuccess
        self.ErrorMessage = None
        self.Properties = None
        if jsonDict:
            self.NextSequenceId = jsonDict['NextSequenceId']
            self.FilterId = jsonDict['FilterId']
            self.ResponseStatus = DSFilterResponseStatus(jsonDict['ResponseStatus'])
            self.ErrorMessage = jsonDict['ErrorMessage']
            self.UpdatesCount = jsonDict['UpdatesCount']
            if jsonDict['Updates'] is not None:
                self.Updates = [DSEconomicChangeCorrection(jsonUpdate) for jsonUpdate in jsonDict['Updates']] 
            self.UpdatesPending = jsonDict['UpdatesPending']
            self.PendingCount = jsonDict['PendingCount']


class EconomicFilters:
    """
    EconomicFilters is the client class that manages the connection to the API server on your behalf.
    It allows you to query for all your custom filters and create/modify new filters.

    Methods Supported
    -----------------
    GetAllFilters: Allows you to query for all the current filters available for your use
    GetFilter: Allows you to download a specific filter and examine the current constituent economic series assigned to the filter.
    CreateFilter: Allows you to create a new economic changes and corrections filter with up to 100K constituent series.
    UpdateFilter: Allows you to update an existing filter, replacing, appending or removing constituent items.
    DeleteFilter: Allows you to remove an existing filter.
    GetEconomicChanges: Allows you to query for any economic changes and corrections using an optional filter.

    Note: You need a Datastream ID which is permissioned to access the Datastream APIs. 
          In addition, this ID also needs to be permissioned to access the custom economic filters service.
          Attempting to access this service without these permissions will result in a permission denied error response.

    Example usage:
    # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
    econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
    # query for all your filters
    filtersResp = econFilterClient.GetAllFilters(DSFilterGetAllAction.AllFilters)
    if filtersResp:
        if filtersResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
            # Your Datastream Id might not be permissioned for managing economic filters on this API
            print('GetAllFilters failed with error ' + filtersResp.ResponseStatus.name + ': ' + filtersResp.ErrorMessage)
        elif filtersResp.Filters and filtersResp.FilterCount > 0:
            # You do have access to some filters
            filters = [[filter.FilterId, filter.OwnerId, 'Yes' if bool(filter.Shared) else 'No', filter.ConstituentsCount, 
                        filter.LastModified.strftime('%Y-%m-%d %H:%M:%S'), filter.Description] for filter in filtersResp.Filters]
            df = pd.DataFrame(data=filters, columns=['FilterId', 'OwnerId', 'Shared', 'Constituents', 'LastModified', 'Description'])
            print(df.to_string(index=False))
        else:
            # You do not have any filters with the specified filter type. Try DSFilterGetAllAction.AllFilters which should return
            # the DATASTREAM_KEYIND_GLOBAL global filter available for download 
            print('GetAllFilters returned zero filters for the authenticated user with the specified DSFilterGetAllAction')

    # sample filter creation
    newFilter = DSEconomicsFilter()
    newFilter.FilterId = 'MyTestFilter'
    newFilter.Constituents = ['CTES85FTA','EOES85FTA','ESES85FTA', 'FNES85FTA']
    newFilter.Description = 'MyTestFilter for testing'
    reqResp = econFilterClient.CreateFilter(newFilter)
    if reqResp:
        if reqResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
            print('GetFilter failed for filter DATASTREAM_KEYIND_GLOBAL with error ' + reqResp.ResponseStatus.name + ': ' + reqResp.ErrorMessage)
        elif reqResp.Filter != None:
            filter = reqResp.Filter
            names = ['FilterId', 'OwnerId', 'Shared?', 'LastModified', 'Description', 'No. of Constituents']
            data = [filter.FilterId, filter.OwnerId, 'Yes' if bool(filter.Shared) else 'No', filter.LastModified.strftime('%Y-%m-%d %H:%M:%S'), filter.Description, filter.ConstituentsCount]
            df = pd.DataFrame(data, index=names)
            print(df)
            print('Constituents:')
            df = pd.DataFrame(filter.Constituents)
            print(df, end='\n\n')

    """

    def __init__(self, config = None, username = None, password = None, proxies = None, sslVerify = None, sslCert = None):
        """
        Constructor: user details can be supplied from a config file or passed directly as parameters in constructor

        1) Using ini file (e.g. config.ini) with format

        [credentials]
        username=YourID
        password=YourPwd

        [proxies]
        # of the form: { 'http' : proxyHttpAddress,  'https' : proxyHttpsAddress } See https://docs.python-requests.org/en/latest/user/advanced/
        proxies=ProxyDetails   

        [cert]
        # option to supply a specific python requests verify option. See https://docs.python-requests.org/en/latest/user/advanced/
        sslVerify=YourCertPath


        # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
        econFilterClient = EconomicFilters('config.ini')

        2) Bypassing a config file and using your credentials directly:

        econFilterClient = EconomicFilters(None, 'YourId', 'YourPwd')

        """

        # Properties
        self.url = "https://product.datastream.com" # Warning: Only override the url for the API service if directed to by LSEG.
        self.username = None
        self.password = None
        self.token = None # when you logon your token for subsequent queries is stored here
        self.tokenExpiry = None # tokens are typically valid for 24 hours. The client will automatically renew the token if you make request within 15 minutes of expiry
        self._proxies = None
        self._sslCert = None
        self._certfiles = None
        # some settings that allow us to track version usage on our servers.
        self._reqSession = requests.Session()
        self._timeout = 180
        self._reqSession.headers['User-Agent'] = self._reqSession.headers['User-Agent'] + DSPackageInfo.UserAgent


        # you can use a config file to specify the user credentials, ssl certificate file, path, etc.
        if (config): 
            parser = configparser.ConfigParser()
            parser.read(config)

            # Warning: Only override the url for the API service if directed to by LSEG.
            if parser.has_option('url', 'path'):
                self.url = self.url if parser.get('url', 'path').strip() == '' else parser.get('url', 'path').strip()
                self.url = self.url.lower()
                if self.url:  # we only support https on the API
                    if re.match("^http:", self.url):
                        self.url = self.url.replace('http:', 'https:', 1)
            
            # you can override the web query timeout value
            if parser.has_option('app', 'timeout'):
                self._timeout = 300 if parser.get('app', 'timeout').strip() == '' else int(parser.get('app', 'timeout').strip())

            # You can optionally provide the Datastream credentials from your config file, or optionally override from the constructor
            if parser.has_option('credentials', 'username'):
                self.username = None if parser.get('credentials', 'username').strip() == '' else parser.get('credentials', 'username').strip()
            if parser.has_option('credentials', 'password'):
                self.password = None if parser.get('credentials', 'password').strip() == '' else parser.get('credentials', 'password').strip()

            # Optionally provide the proxies details from the config file also
            if parser.has_option('proxies', 'proxies'):
                configProxies = None if parser.get('proxies','proxies').strip() == '' else parser.get('proxies', 'proxies').strip()
                if configProxies:
                    self._proxies = ast.literal_eval(configProxies)

            # Optionally specify a specific server CA file or path from the config
            if parser.has_option('cert', 'sslVerify'):
                configCert = None if parser.get('cert','sslVerify').strip() == '' else parser.get('cert', 'sslVerify').strip()
                if configCert:
                    self._certfiles = configCert


        # set the full reference to the API service from the supplied hostname
        self.url = self.url +'/DSWSClient/V1/DSEconomicsFilterService.svc/rest/'

        # You can also override any config by specifying your user credentials, proxy or ssl certificate as parameters in the constructor
        # proxy input must be of the form:
        # proxies = { 'http' : proxyHttpAddress,  'https' : proxyHttpsAddress } # see https://docs.python-requests.org/en/latest/user/advanced/
        if proxies:
            self._proxies = proxies

        if sslCert: # option to specify specific client ssl certificate file in constructor
            self._sslCert = sslCert

        if sslVerify: # option to specify specific server root CA file in constructor
            self._certfiles = sslVerify

        if self._certfiles == None: # get default CA file
            # Load windows certificates to a local file for Windows platform
            pf = platform.platform()
            if pf.upper().startswith('WINDOWS'):
                # SSL can be used on Linux and Windows unlike the wincertstore which worked on Win Platform only
                self._certfiles = "tempCertFile.pem"
                import ssl
                with open(self._certfiles, "w+",) as file_obj:
                    for store in ["CA", "ROOT", "MY"]:
                        for cert, encoding, trust in ssl.enum_certificates(store):
                            certificate = ssl.DER_cert_to_PEM_cert(cert)
                            file_obj.write(certificate)
                    file_obj.close()
            else:
                self._certfiles = requests.certs.where()

        # any user credentials loaded from the config file can be over-ridden from credentials supplied as constructor parameters
        if username:
            self.username = username
        if password:
            self.password = password

        # with the given user credentials, we try and logon to the API service to retrieve a token for use with all subsequent queries
        # Must be some credentials supplied and not the stub credentials
        if isinstance(self.username, str) and len(self.username) > 0 and self.username != 'YourID' and isinstance(self.password, str) and len(self.password) > 0:
            self._get_Token()
        else:
            raise Exception("You must supply some user credentials.")


    def _get_Token(self):
        """
        _get_Token uses you credentials to try and obtain a token to be used in subsequent request for data. The returned token is valid for 24 hours
        """
        try:
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters._get_Token', 'Requesting new token.')
            token_url = self.url + 'GetToken'
            tokenReq = { "Password" : self.password,
                         "Properties" : [{ "Key" : "__AppId", "Value" : DSPackageInfo.appId}],
                         "UserName" : self.username}
            #Post Token Request
            json_Response = self._get_json_Response(token_url, tokenReq)
            self.tokenExpiry = DSUserObjectDateFuncs.jsonDateTime_to_datetime(json_Response['TokenExpiry'])
            self.token = json_Response['TokenValue']
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters._get_Token', 'New token received.')
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters._get_Token', 'Exception occured.', exp)
            raise exp

    def IsValid(self):
        return isinstance(self.token, str) and len(self.token) > 0 and isinstance(self.tokenExpiry, datetime)

    def Check_Token(self):
        if not self.IsValid():
            raise Exception("You are not logged on. Please recreate the EconomicFilters client supplying valid user credentials.")
        # A function called before every query to check and renew the token if within 15 minutes of expiry time or later
        timeRenew = datetime.utcnow() + timedelta(minutes = 15) # curiously utcnow() method doesn't set the time zone to utc. We need to do so to compare with token.
        timeRenew = datetime(timeRenew.year, timeRenew.month, timeRenew.day, timeRenew.hour, timeRenew.minute, timeRenew.second, 0, tzinfo=pytz.utc)
        if self.tokenExpiry <= timeRenew : 
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogTrace, 'DatastreamPy', 'EconomicFilters.Check_Token', 'Token has expired. Refrefreshing')
            self._get_Token()


    def _json_Request(self, reqObject):
        # An internal method to convert the request object into JSON for sending to the API service
        try:
            #convert the dictionary (raw text) to json text first, encoding any datetimes as json /Date() objects
            jsonText = json.dumps(reqObject, cls = DSEconFilterJsonDateTimeEncoder)
            byteTemp = bytes(jsonText,'utf-8')
            byteTemp = jsonText.encode('utf-8')
            #convert the json Text to json formatted Request
            jsonRequest = json.loads(byteTemp)
            return jsonRequest
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters._json_Request', 'Exception occured:', exp)
            raise exp


    def _get_Response(self, reqUrl, raw_request):
        # An internal method to perform a request against the API service.
        #convert raw request to json format before post
        jsonRequest = self._json_Request(raw_request)

        # post the request
        DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogTrace, 'DatastreamPy', 'EconomicFilters._get_Response', 'Starting web request:', raw_request)
        httpResponse = self._reqSession.post(reqUrl, json = jsonRequest,  proxies = self._proxies, verify = self._certfiles, cert = self._sslCert, timeout = self._timeout)
        return httpResponse

        
    def _get_json_Response(self, reqUrl, raw_request):
        # This method makes the query and does some basic error handling
        try:
            # convert the request to json and post the request
            httpResponse = self._get_Response(reqUrl, raw_request)

            # check the response
            if httpResponse.ok:
                json_Response = dict(httpResponse.json())
                DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogTrace, 'DatastreamPy', 'EconomicFilters._get_json_Response', 'Web response received:', json_Response)
                return json_Response
            elif httpResponse.status_code == 400 or httpResponse.status_code == 403:
                # possible DSFault exception returned due to permissions, etc
                try:
                    tryJson = json.loads(httpResponse.text)
                    if 'Message' in tryJson.keys() and 'Code' in tryJson.keys():
                        faultDict = dict(tryJson)
                        DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters._get_json_Response', 'API service returned a DSFault:', 
                                                            faultDict['Code'] + ' - ' + faultDict['Message'])
                        raise DSUserObjectFault(faultDict)
                except json.JSONDecodeError as jdecodeerr:
                    pass
            # unexpected response so raise as an error
            httpResponse.raise_for_status()
        except json.JSONDecodeError as jdecodeerr:
            DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters._get_json_Response', 'JSON decoder Exception occured:', jdecodeerr.msg)
            raise
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters._get_json_Response', 'Exception occured:', exp)
            raise
 

    def __CheckConstituents(self, constituents, updateAction):
        # We perform some basic sanity checks on the constituents sent to the service
        if updateAction < DSFilterUpdateActions.UpdateDescription: # must have some constituents for create, append, replace or delete
            if constituents == None or not isinstance(constituents, list):
                return 'The filter Constituents property must be a list containing at least one economic series with a maximum limit of 100K items.'
            if len(constituents) == 0 or len(constituents) > 100000:
                return 'The filter Constituents property must contain at least one economic series with a maximum limit of 100K items.'
        elif constituents != None and not isinstance(constituents, list): #update description or share type must provide None or at least a list of series (ignored)
           return 'The filter Constituents property must be a list object.'
        return None

       
    def __CheckFilterId(self, filterId):
        # The requested filter ID must match the specification of between 5 and 45 alphanumeric or underscore characters
        if filterId == None or not re.match('^[A-Z0-9_]{5,45}$', filterId, re.I):
            return 'The filter identifier must be between 5 and 45 characters long and contain only alphanumeric or underscore characters.'
        return None #valid

          
    def __VerifyEconomicFilter(self, econFilter, updateAction, isCreate):
        # must be a DSEconomicsFilter and the filer ID and constituents must be valid
        if not isinstance(econFilter, DSEconomicsFilter):
            return 'EconomicFilters CreateFilter or ModifyFilter methods require a valid DSEconomicsFilter instance.'
        if not isCreate and (not isinstance(updateAction, DSFilterUpdateActions) or updateAction == DSFilterUpdateActions.CreateFilter):
            return 'EconomicFilters ModifyFilter cannot be called with the CreateFilter flag.'

        # must have valid Id and some constituents
        resp = self.__CheckFilterId(econFilter.FilterId)
        if resp is None:
            ## check constituents if appending replacing or removing
            resp = self.__CheckConstituents(econFilter.Constituents, updateAction)

        # finally ensure other fields which are only set on returning a response are valid defaults for the subsequent web query
        if resp is None:
            econFilter.SetSafeUpdateParams()
        return resp  # will contain an error message if invalid params else None


    def GetAllFilters(self, getType = DSFilterGetAllAction.AllFilters):
        """ GetAllFilters returns all the current filters you can use in queries for economic changes and corrections

            Example usage:
            # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
            econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
            # query for all your filters
            filtersResp = econFilterClient.GetAllFilters(DSFilterGetAllAction.AllFilters)
            if filtersResp:
                if filtersResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
                    # Your Datastream Id might not be permissioned for managing economic filters on this API
                    print('GetAllFilters failed with error ' + filtersResp.ResponseStatus.name + ': ' + filtersResp.ErrorMessage)
                elif filtersResp.Filters and filtersResp.FilterCount > 0:
                    # You do have access to some filters
                    filters = [[filter.FilterId, filter.OwnerId, 'Yes' if bool(filter.Shared) else 'No', filter.ConstituentsCount, 
                                filter.LastModified.strftime('%Y-%m-%d %H:%M:%S'), filter.Description] for filter in filtersResp.Filters]
                    df = pd.DataFrame(data=filters, columns=['FilterId', 'OwnerId', 'Shared', 'Constituents', 'LastModified', 'Description'])
                    print(df.to_string(index=False))
                else:
                    # You do not have any filters with the specified filter type. Try DSFilterGetAllAction.AllFilters which should return
                    # the DATASTREAM_KEYIND_GLOBAL global filter available for download 
                    print('GetAllFilters returned zero filters for the authenticated user with the specified DSFilterGetAllAction')
        """

        try:
            if not isinstance(getType, DSFilterGetAllAction):
                resp = DSEconomicsFilterGetAllResponse()
                resp.ResponseStatus = DSFilterResponseStatus.FilterFormatError
                resp.ErrorMessage = 'EconomicFilters GetAllFilters method requires a valid DSFilterGetAllAction type.'
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters.GetAllFilters', 'Error: ' + resp.ErrorMessage)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.GetAllFilters', 'GetAllFilters requested.')
            self.Check_Token() # check and renew token if within 15 minutes of expiry
            allFilters_url = self.url + 'GetAllFilters'
            raw_request = { "GetTypes" : getType,
                            "Properties" : None,
                            "TokenValue" : self.token}
            json_Response = self._get_json_Response(allFilters_url, raw_request)
            response = DSEconomicsFilterGetAllResponse(json_Response)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.GetAllFilters', 'GetAllFilters returned response.')
            return response
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters.GetAllFilters', 'Exception occured.', exp)
            raise exp


    def GetFilter(self, filterId):
        """ GetFilter returns the details for an individual filter

            Example usage:
            # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
            econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
            filterResp = econFilterClient.GetFilter('DATASTREAM_KEYIND_GLOBAL')
            if filterResp:
                if filterResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
                    print('GetFilter failed for filter DATASTREAM_KEYIND_GLOBAL with error ' + filterResp.ResponseStatus.name + ': ' + filterResp.ErrorMessage)
                elif filterResp.Filter != None:
                    filter = filterResp.Filter
                    names = ['FilterId', 'OwnerId', 'Shared?', 'LastModified', 'Description', 'No. of Constituents']
                    data = [filter.FilterId, filter.OwnerId, 'Yes' if bool(filter.Shared) else 'No', filter.LastModified.strftime('%Y-%m-%d %H:%M:%S'), filter.Description, filter.ConstituentsCount]
                    df = pd.DataFrame(data, index=names)
                    print(df)

                    print('Constituents:')
                    df = pd.DataFrame(filter.Constituents)
                    print(df, end='\n\n')
        """
        try:
            # check validity of requested filter Id
            filterchk = self.__CheckFilterId(filterId)
            if filterchk is not None:
                resp = DSEconomicsFilterResponse()
                resp.ResponseStatus = DSFilterResponseStatus.FilterFormatError
                resp.ErrorMessage = filterchk
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters.GetFilter', 'Error: ' + filterchk)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.GetFilter', 'Requesting filter ' + filterId)
            self.Check_Token() # check and renew token if within 15 minutes of expiry
            filter_url = self.url + 'GetFilter'
            raw_request = { "TokenValue" : self.token,
                            "FilterId" : filterId,
                            "Properties" : None}
            json_Response = self._get_json_Response(filter_url, raw_request)
            response = DSEconomicsFilterResponse(json_Response)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.GetFilter', 'Filter ' + filterId + ' returned a response.')
            return response
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters.GetFilter', 'Exception occured.', exp)
            raise exp


    def CreateFilter(self, newFilter):
        """ CreateFilter allows you to create a custom filter

            Example usage:
            # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
            econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
            newFilter = DSEconomicsFilter()
            newFilter.FilterId = 'MyTestFilter'
            newFilter.Constituents = ['CTES85FTA','EOES85FTA','ESES85FTA', 'FNES85FTA']
            newFilter.Description = 'MyTestFilter for testing'
            filterResp = econFilterClient.CreateFilter(newFilter)
            if filterResp:
                if filterResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
                    print('CreateFilter failed with error ' + filterResp.ResponseStatus.name + ': ' + filterResp.ErrorMessage)
                elif filterResp.Filter != None:
                    filter = filterResp.Filter
                    names = ['FilterId', 'OwnerId', 'Shared?', 'LastModified', 'Description', 'No. of Constituents']
                    data = [filter.FilterId, filter.OwnerId, 'Yes' if bool(filter.Shared) else 'No', filter.LastModified.strftime('%Y-%m-%d %H:%M:%S'), filter.Description, filter.ConstituentsCount]
                    df = pd.DataFrame(data, index=names)
                    print(df)

                    print('Constituents:')
                    df = pd.DataFrame(filter.Constituents)
                    print(df, end='\n\n')

                    if filterResp.ItemErrors and len(filterResp.ItemErrors):
                    print('Some constituents were not added due to invalid format or they do not exist :')
                    df = pd.DataFrame(filterResp.ItemErrors)
                    print(df, end='\n\n')
        """
        try:
            #pre check the validity of the filter that needs to be created
            filterchk = self.__VerifyEconomicFilter(newFilter, DSFilterUpdateActions.CreateFilter, True)
            if filterchk is not None:
                resp = DSEconomicsFilterResponse()
                resp.ResponseStatus = DSFilterResponseStatus.FilterFormatError
                resp.ErrorMessage = filterchk
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters.CreateFilter', 'Error: ' + filterchk)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.CreateFilter', 'Creating filter ' + newFilter.FilterId)
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            create_url = self.url + "CreateFilter"
            raw_request = { "Filter" : newFilter.__dict__,
                            "Properties" : None,
                            "TokenValue" : self.token,
                            "UpdateAction" : DSFilterUpdateActions.CreateFilter}
            json_Response = self._get_json_Response(create_url, raw_request)
            response = DSEconomicsFilterResponse(json_Response)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.CreateFilter', 'Filter ' + newFilter.FilterId + ' returned a response.')
            return response
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters.CreateFilter', 'Exception occured.', exp)
            raise exp


    def UpdateFilter(self, filter, updateAction):
        """ UpdateFilter allows you to update an existing custom filter

            Example usage:
            # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
            econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
            newFilter = DSEconomicsFilter()
            newFilter.FilterId = 'MyTestFilter'  # assumes the filter already exists
            newFilter.Constituents = ['FRES85FTA', 'GRES85FTA', 'HNES85FTA', 'POES85FTA']
            filterResp = econFilterClient.UpdateFilter(newFilter, DSFilterUpdateActions.AppendConstituents)
            if filterResp:
                if filterResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
                    print('UpdateFilter failed with error ' + filterResp.ResponseStatus.name + ': ' + filterResp.ErrorMessage)
                elif filterResp.Filter != None:
                    filter = filterResp.Filter
                    names = ['FilterId', 'OwnerId', 'Shared?', 'LastModified', 'Description', 'No. of Constituents']
                    data = [filter.FilterId, filter.OwnerId, 'Yes' if bool(filter.Shared) else 'No', filter.LastModified.strftime('%Y-%m-%d %H:%M:%S'), filter.Description, filter.ConstituentsCount]
                    df = pd.DataFrame(data, index=names)
                    print(df)

                    print('Constituents:')
                    df = pd.DataFrame(filter.Constituents)
                    print(df, end='\n\n')

                    if filterResp.ItemErrors and len(filterResp.ItemErrors):
                    print('Some constituents were not added due to invalid format or they do not exist :')
                    df = pd.DataFrame(filterResp.ItemErrors)
                    print(df, end='\n\n')
        """
        try:
            #pre check the validity of the filter that needs to be created
            filterchk = self.__VerifyEconomicFilter(filter, updateAction, False)
            if filterchk is not None:
                resp = DSEconomicsFilterResponse()
                resp.ResponseStatus = DSFilterResponseStatus.FilterFormatError
                resp.ErrorMessage = filterchk
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters.UpdateFilter', 'Error: ' + filterchk)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.UpdateFilter', 'Updating filter ' + filter.FilterId)
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            update_url = self.url + 'UpdateFilter'
            raw_request = { "Filter" : filter.__dict__,
                            "Properties" : None,
                            "TokenValue" : self.token,
                            "UpdateAction" : updateAction}
            json_Response = self._get_json_Response(update_url, raw_request)
            response = DSEconomicsFilterResponse(json_Response)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.UpdateFilter', 'Filter ' + filter.FilterId + ' returned a response.')
            return response
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters.UpdateFilter', 'Exception occured.', exp)
            raise exp

    
    def DeleteFilter(self, filterId):
        """ DeleteFilter allows you to delete an existing custom filter

            Example usage:
            # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
            econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
            filterResp = econFilterClient.DeleteFilter('MyTestFilter') # assumes the filter already exists
            if filterResp:
                if filterResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
                    print('DeleteFilter failed with error ' + filterResp.ResponseStatus.name + ': ' + filterResp.ErrorMessage)
                else: # No filter object will be returned
                    print('The filter was successfully deleted.')
        """
        try:
            # check validity of requested filter Id
            filterchk = self.__CheckFilterId(filterId)
            if filterchk is not None:
                resp = DSEconomicsFilterResponse()
                resp.ResponseStatus = DSFilterResponseStatus.FilterFormatError
                resp.ErrorMessage = filterchk
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters.DeleteFilter', 'Error: ' + filterchk)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.DeleteFilter', 'Deleting filter ' + filterId)
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            delete_url = self.url + 'DeleteFilter'
            raw_request = { "FilterId" : filterId,
                            "Properties" : None,
                            "TokenValue" : self.token}

            json_Response = self._get_json_Response(delete_url, raw_request)
            response = DSEconomicsFilterResponse(json_Response)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.DeleteFilter', 'DeleteFilter (' + filterId + ') returned a response.')
            return response
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters.DeleteFilter', 'Exception occured.', exp)
            raise exp


    def GetEconomicChanges(self, startDate = None, sequenceId = 0, filter = None):
        """ GetEconomicChanges allows you to query for any economic changes and corrections, returning a DSEconomicChangesResponse if successful.

        There are two modes of usage:
        1) Get sequence ID for changes on or after a given date. The service supports querying for changes from a given date up to 28 days in the past.
           Supplying a timestamp will query for the sequence ID to use in subsequent queries to retrieve any changes. The query returns a boolean property
           indicating if there are any updates available and also returns the number of outstanding updates available using the returned sequence ID 
           in the PendingCount property of the response.

           The earliest sequence returned will always be a maximum of 28 days in the past. If you supply no datetime, and the sequenceID is left as 0 (default),
           then the returned sequence ID will represent the first update from midnight on the prior working weekday (e.g. request on a Monday, and the 
           startDate will be assumed to be 00:00:00 on the prior Friday). Request with a date in the future will return the sequence ID for the next update 
           that will occur in the future.

           Example Usage:
            # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
            econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
            # supplying no datetime will search for updates from the start of the prior working (week)day.
            updatesResp = econFilterClient.GetEconomicChanges() # Get sequence ID for any updates beginning the previous working weekday
            # alternatively requesting sequence for any changes that occurred from 5 days ago
            updatesResp = econFilterClient.GetEconomicChanges(datetime.today() - timedelta(days=5)) # Get sequence ID starting 5 days ago

            # the above should tell us the start sequence ID for updates from the given start datetime and how many updates we have pending
            sequenceId = 0
            if updatesResp:
                if updatesResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
                    print('GetEconomicChanges failed with error ' + updatesResp.ResponseStatus.name + ': ' + updatesResp.ErrorMessage)
                else:
                    print('GetEconomicChanges returned the first update sequence as {}.'.format(updatesResp.NextSequenceId))
                    print('You have {} updates pending starting from {}.'.format(updatesResp.PendingCount, updatesResp.NextSequenceId))
                    sequenceId = updatesResp.NextSequenceId

        2) With a given sequence ID, retrieve all the series that updated after the given sequence ID. Note the supplied startDate must be None. This will return a chain
           of responses with each response containing up to 10K updates and the next sequence in the chain to request. Whilst each response returns UpdatesPending
           as True, there are more updates to be requested. When UpdatesPending is returned as False it indicates that the given response is the last response with
           updates. The NextSequenceId property in the final update is the next ID that will be assigned on any subsequent new updates occurring. It should be used in 
           polling for updates at a minimum frequency of every 10 minutes.

           The request also accepts an optional custom filter to restrict the list of returned updates to just the series that comprise the constituents of the filter.

            Example usage without a filter:
            # first logon with your credentials. Creating an EconomicFilters instance with your credentials automatically logs on for you 
            econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')
            # using a sequence ID retrieved using the first mode of requesting GetEconomicChanges
            updatesResp = econFilterClient.GetEconomicChanges(None, sequenceId)
            if updatesResp:
                if updatesResp.ResponseStatus != DSFilterResponseStatus.FilterSuccess:
                    print('GetEconomicChanges failed with error ' + updatesResp.ResponseStatus.name + ': ' + updatesResp.ErrorMessage)
                else:
                    # You have some updates; process them.
                    print ('You have {} new updates:'.format(updatesResp.UpdatesCount))
                    updates = [[update.Series, update.Frequency.name, update.Updated.strftime('%Y-%m-%d %H:%M:%S')] for update in updatesResp.Updates]
                    df = pd.DataFrame(data=updates, columns=['Series', 'Frequency', 'Updated'])
                    print(df, end='\n\n')
                    if updatesResp.UpdatesPending:
                        print ('You still have {} updates pending starting from new sequence ID {}.'.format(updatesResp.PendingCount, updatesResp.NextSequenceId))
                    else:
                        print ('You have no more updates pending. Use the new sequence {} to poll for future updates.'.format(updatesResp.NextSequenceId))

            Example usage with a filter which will return a smaller refined subset of updates restricted to the constituents of the filter
            updatesResp = econFilterClient.GetEconomicChanges(None, sequenceId, 'DATASTREAM_KEYIND_GLOBAL')
            # and process as above

        """
        try:
            reqCheck = None
            # check validity of inputs
            if startDate is not None:
                if not (isinstance(startDate, date) or isinstance(startDate, datetime)):
                    reqCheck = 'startDate, if supplied, must be a date or datetime object.'
            elif sequenceId is not None:
                if not isinstance(sequenceId, int):
                    reqCheck = 'sequenceId, if supplied, must be an integer specifying the start sequence for updates.'
                elif sequenceId > 0 and (filter is not None) and not isinstance(filter, str):
                    reqCheck = 'filter, if supplied, must be a string specifying the filter of constituents to check for updates against.'
            # return an error response if we encountered any invalid inputs
            if reqCheck is not None:
                resp = DSEconomicChangesResponse()
                resp.ResponseStatus = DSFilterResponseStatus.FilterError
                resp.ErrorMessage = reqCheck
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'EconomicFilters.GetEconomicChanges', 'Error: ' + reqCheck)
                return resp

            stringReq = None
            if startDate is not None:
                stringReq = 'Requesting sequence ID for updates from ' + startDate.strftime('%Y-%m-%d %H:%M:%S') + '.'
            elif isinstance(sequenceId, int) and sequenceId > 0:
                stringReq = 'Requesting updates from sequence ID {}'.format(sequenceId)
                if isinstance(filter, str) and len(filter) > 0:
                    stringReq = stringReq + ' with filter {}.'.format(filter)
                else:
                    stringReq = stringReq + '.'
            else: # no parameters at all requests the default
                stringReq = 'Requesting default updates sequence (from prior working day).'

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.GetEconomicChanges', stringReq)
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            filter_url = self.url + 'GetEconomicChanges'
            raw_request = { "TokenValue" : self.token,
                            "StartDate" : startDate,
                            "SequenceId" : sequenceId if (startDate is None) and isinstance(sequenceId, int) else 0,
                            "Filter" : filter if (startDate is None) and isinstance(sequenceId, int) and sequenceId > 0 else None,
                            "Properties" : None}

            json_Response = self._get_json_Response(filter_url, raw_request)
            response = DSEconomicChangesResponse(json_Response)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'EconomicFilters.GetEconomicChanges', 'GetEconomicChanges request returned a response.')
            return response
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'EconomicFilters.GetEconomicChanges', 'Exception occured.', exp)
            raise exp

            




