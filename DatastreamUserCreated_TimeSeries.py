"""
Datastream User Created Timeseries
----------------------------------

Datastream permits users to create and manage custom items. One of these types is custom timeseries data. Clients can upload their own timeseries data to 
Datastream’s central systems. These can then be used in combination with Datastream maintained series in charts and data requests with the full power of the 
Datastream expressions language.

This file defines the TimeseriesClient and ancillary classes that assist in creating and managing timeseries data.

TimeseriesClient is derived from the DSConnect class that manages the raw JSON query and response protocol for the API service.
"""

import json
import math
import pytz
from datetime import datetime, timedelta, date
from .DSUserDataObjectBase import *
from .DSConnect import DSConnect

class DSTimeSeriesFrequencyConversion(IntEnum):
    """
    This enumeration is a supporting attribute for the FrequencyConversion properties of the DSTimeSeriesRequestObject and DSTimeSeriesResponseObjects.
    This enumeration specifies how to return values if your end users requests the timeseries at a lower frequency. For example, if your timeseries has daily
    frequency data, and your user requests monthly or quarterly data, the FrequencyConversion property instructs the mainframe how to return monthly or quarterly
    data.
    """
    EndValue = 0  # The daily value for the end of the requested period will be returned.
    AverageValue = 1 # The average of all the values for the requested period will be returned.
    SumValues = 2 # The sum of all the values for the requested period will be returned. For example, GDP is usually reported quarterly. 
                  # Use this setting to return the sum for the 4 quarterly GDP periods should the user request annual values.
    ActualValue = 3 # The actual value for the requested start date will be returned for the same given date in the requested period.

class DSTimeSeriesDateAlignment(IntEnum):
    """
    This enumeration is a supporting attribute for the DateAlignment properties of the DSTimeSeriesRequestObject and DSTimeSeriesResponseObjects.
    When you supply monthly, quarterly or annual data, the dates are stored internally as the first day of the given period and always returned to you through this 
    interface as the first date of the given period. However, when your users request data from Datastream, you can specify whether the dates returned to users are
    returned with dates set as the start, mid or end of the requested period
    """
    EndPeriod = 0   # This will return dates to your users that represent the last day of the month, quarter or year.
    StartPeriod = 1 # This will return dates to your users that represent the first day of the month, quarter or year.
    MidPeriod = 2   # This will return dates to your users that represent the middle of the month (15th day), quarter (15th of the middle month) or year (30th June).

class DSTimeSeriesCarryIndicator(IntEnum):
    """
    This enumeration is a supporting attribute for the CarryIndicator properties of the DSTimeSeriesRequestObject and DSTimeSeriesResponseObjects.
    When you supply data which contains 'Not A Number' values (NaN or None) to denote non trading days, this enum instructs the mainframe in how to store the values.
    """
    Yes = 0  # Any incoming NaN values are replaced with the last non-NaN value (e.g. 1,2,3,NaN,5,NaN,7,8 will be converted and stored as 1,2,3,3,5,5,7,8).  
    No = 1   # Any incoming NaN values are stored as is and returned as NaN values.
    Pad = 2  # This is similar to YES, but also pads the final value for any dates your users may request beyond the last date in your timeseries.
             # For example, if your timeseries supplies just 3 values 1, NaN and 3, and your user requests a range of dates two days before and two days after your 
             # range, your user will receive the following values:
             # No:  NaN, NaN, 1, NaN, 3, NaN, NaN
             # Yes: NaN, NaN, 1, 1, 3, NaN, NaN
             # Pad: NaN, NaN, 1, 1, 3, 3, 3

class DSTimeSeriesDataInput:
    """
    This class is a supporting attribute for the DateInput property of the DSTimeSeriesRequestObject. It is used to supply the raw data for the 
    timeseries.

    Properties
    ----------
    StartDate: A datetime value defining the start date for the timeseries.
    EndDate: A datetime value defining the end date for the timeseries. See note above. This is used internally by Datastream for logging purposes only.
    Frequency: The frequency of the timeseries. One of the DSUserObjectFrequency values defined in DSUserDataObjectBase.py
    Values: An array of float values. Use None to represent NotANumber for non-trading days. Alternatively, if you set the TimeseriesClient 
            property useNaNforNotANumber as True, you can use float NaN values. 

    Note: Datastream takes the StartDate, Frequency and Values properties defined here and creates the timeseries based only on these parameters. 
    The EndDate is not actually used internally other than for logging purposes. The true end date is calculated based on the start date, frequency and the 
    supplied list of values. Supply too few or too many values and the mainframe will accept them and set the end date accordingly based on the given frequency 
    for the item. 
    """
    def __init__(self, startDate = None, endDate = None, frequency = DSUserObjectFrequency.Daily, values = None):
        self.StartDate = startDate
        self.EndDate = endDate
        self.Frequency = frequency
        self.Values = values

class DSTimeSeriesDateRange:
    """
    This class is a supporting attribute for the DateRange property of the DSTimeSeriesResponseObject. It returns the raw data for the timeseries.

    Properties
    ----------
    Dates: A list of datetime values specifying the dates for each datapoint.
    Values: A list of float values specifying the values for each datapoint.
    ValuesCount: A count of the number of datapoints in the timeseries.

    Note: The DateRange property of the DSTimeSeriesResponseObject always returns the dates for a given frequency as the first date in each period 
    (e.g. 2022-01-01, 2020-04-01, etc. for quarterly frequencies). You specify whether you want your users to receive either the first, mid or end dates
    in the given period by setting the DateAlignment property (DSTimeSeriesDateAlignment) of the DSTimeSeriesRequestObject.
    
    When you retrieve a list of all your available timeseries using the GetAllItems method, since this list could contain many thousand timeseries objects,
    the Dates and Values lists will always be None. Only the ValuesCount field will be set to reflect the number of datapoints available for each item. You need to 
    request an individual timeseries (GetItem method) in order to receive a response containing actual data in the Dates and Values properties.
    """
    def __init__(self, jsonDict, convertNoneToNans = False):
        self.Dates = None
        self.Values = None
        self.ValuesCount = 0
        if jsonDict:
            self.ValuesCount = jsonDict['ValuesCount']
            # GetAllItems queries return a list of timeseries objects but don't populate the Dates and Values properties for the items
            if jsonDict['Dates']: # convert the json Dates to datetime
                self.Dates = jsonDict['Dates']
                for i in range(len(self.Dates)):
                    self.Dates[i] = DSUserObjectDateFuncs.jsonDateTime_to_datetime(self.Dates[i])
            if jsonDict['Values']: # if user wants NaNs rather Nones, then we need to add step to check and convert array
                self.Values = jsonDict['Values'] if not convertNoneToNans else [math.nan if val is None else val for val in jsonDict['Values']]


class DSTimeSeriesDateInfo:
    """
    This class is a supporting attribute for the DateInfo property of the DSTimeSeriesResponseObject. It describes the basic range of data for the timeseries.

    Properties
    ----------
    StartDate: A datetime value defining the start date of the timeseries data
    Enddate: A datetime value defining the end date for the timeseries.
    Frequency: The frequency of the timeseries. One of the DSUserObjectFrequency values defined in DSUserDataObjectBase.py

    Note: The DateRange property (DSTimeSeriesDateRange described above) of the DSTimeSeriesResponseObject always returns the dates for a given frequency
    as the first date in each period (e.g. 2022-01-01, 2020-04-01, etc. for quarterly frequencies). However, The StartDate and EndDate values returned
    in this class for the DSTimeSeriesResponseObject reflect the start and end dates of the range of dates that would be returned to users requesting the data
    via Datastream For Office, charting, etc. This depends on the DateAlignment property (DSTimeSeriesDateAlignment) of the timeseries. The start and end dates 
    returned here will be either the start, mid or end dates for the set frequency based on the DateAlignment property (see DSTimeSeriesDateAlignment).
    """
    def __init__(self, jsonDict):
        self.StartDate = None
        self.EndDate = None   # # Defines the end date of the timeseries data
        self.Frequency = DSUserObjectFrequency.Daily # Defines the frequency of the timeseries data
        if jsonDict:
            self.StartDate = DSUserObjectDateFuncs.jsonDateTime_to_datetime(jsonDict['StartDate'])
            self.EndDate = DSUserObjectDateFuncs.jsonDateTime_to_datetime(jsonDict['EndDate'])
            self.Frequency = DSUserObjectFrequency(jsonDict['Frequency'])


class DSTimeSeriesUserObjectBase(DSUserObjectBase):
    """
    DSTimeSeriesUserObjectBase is the base object for creating or requesting timeseries data. It has two subclasses DSTimeSeriesRequestObject and
    DSTimeSeriesResponseObject. It defines the basic attributes for a timeseries. It subclasses DSUserObjectBase which defines the basic attributes common
    to all five user created item types supported by the API.

    Specifics of some of the properties of the DSUserObjectBase superclass
    ----------------------------------------------------------------------
    ID: The ID property is defined in DSUserObjectBase but has a specific format for timeseries. Timeseries IDs must be 8 alphanumeric characters long, start with TS
        followed by 6 uppercase alphanumeric characters. For example: TSTEST01, TS123456, TSMYTEST, etc.
    Mnemonic: The Mnemonic property is defined in DSUserObjectBase but should always be left empty or set the same as the ID property for timeseries requests.
              As a safety measure, this class always ensures it's the same as the ID. In a response from the API server, the value will always be the same as the ID.
    (see DSUserObjectBase for a description of the other properties)

    DSTimeSeriesUserObjectBase specific properties
    ----------------------------------------------
    Management Group: This is an optional group name that allows you to organise timeseries into distinct 'folders' displayed in the search category of Navigator.
                      This can be up to 10 uppercase alphanumeric characters. Leave blank for the item to be assigned under the 'GENERAL' group.
    Units: This is a optional qualifying unit for your data. For example: tons, U$ millions, index, etc. Maximum 12 characters.
    DecimalPlaces: A numeric value between 0 and 8 decimal places specifying how many decimal places to use when storing data. The maximum length including decimals
                   for a value is 10 characters including the decimal point. Boundary case examples are 0.12345678, 1234567890, 123456789.0, etc.
    FrequencyConversion: A DSTimeSeriesFrequencyConversion enum value specifying how to return values if a user requests data at a lower frequency than the timeseries
                         data is supplied. See DSTimeSeriesFrequencyConversion for details.
    DateAlignment: A DSTimeSeriesDateAlignment enum value specifying whether dates for certain frequencies should be returned as the start, middle or end date of 
                   the period. See DSTimeSeriesDateAlignment for details.
    CarryIndicator: A DSTimeSeriesCarryIndicator enum value specifying how to treat 'Not A Number' values for non-trading days and how to represent values if users
                    request data after the end of the timeseries range. See DSTimeSeriesCarryIndicator for details.
    PrimeCurrencyCode: An optional 2 character currency code for your timeseries.

    Deprecated Properties
    ---------------------
    HasPadding: This property has been replaced with the CarryIndicator property and will always be False
    UnderCurrencyCode: This property has been deprecated and will always return None
    AsPercentage: This This property has been deprecated and will always return False
    """
    def __init__(self, jsonDict):
        super().__init__(jsonDict)
        self.ManagementGroup = "GENERAL"
        self.Units = None
        self.DecimalPlaces = 0
        self.FrequencyConversion = DSTimeSeriesFrequencyConversion.EndValue
        self.DateAlignment = DSTimeSeriesDateAlignment.EndPeriod
        self.CarryIndicator = DSTimeSeriesCarryIndicator.Yes
        self.PrimeCurrencyCode = None
        self.UnderCurrencyCode = None
        self.HasPadding = False
        self.AsPercentage = False
        if jsonDict:
            self.ManagementGroup = jsonDict['ManagementGroup']
            self.Units = jsonDict['Units']
            self.DecimalPlaces = jsonDict['DecimalPlaces']
            self.AsPercentage = jsonDict['AsPercentage']
            self.FrequencyConversion = DSTimeSeriesFrequencyConversion(jsonDict['FrequencyConversion'])
            self.DateAlignment = DSTimeSeriesDateAlignment(jsonDict['DateAlignment'])
            self.CarryIndicator = DSTimeSeriesCarryIndicator(jsonDict['CarryIndicator'])
            self.PrimeCurrencyCode = jsonDict['PrimeCurrencyCode']
            # Deprecated properties
            self.UnderCurrencyCode = None
            self.HasPadding = False
            self.AsPercentage = False


class DSTimeSeriesRequestObject(DSTimeSeriesUserObjectBase):
    """
    DSTimeSeriesRequestObject is a subclass of DSTimeSeriesUserObjectBase and is used to create or modify a timeseries.

    See DSTimeSeriesUserObjectBase for details of all the superclass properties.

    Properties
    ----------
    DataInput: A DSTimeSeriesDataInput object used to supply the start date, end date, frequency and list of data values. See DSTimeSeriesDataInput for details.
    """
    def __init__(self, id = None, startDate = None, endDate = None, frequency = None, values = None):
        """ A static constructor that populates the critical fields for you for a new timeseries"""
        super().__init__(None)
        self.Id = id
        self.DataInput = DSTimeSeriesDataInput(startDate, endDate, frequency, values)


class DSTimeSeriesResponseObject(DSTimeSeriesUserObjectBase):
    """
    DSTimeSeriesResponseObject is a subclass of DSTimeSeriesUserObjectBase and is used to return the details for a timeseries.

    See DSTimeSeriesUserObjectBase for details of all the superclass properties.

    Properties
    ----------
    DateInfo: A DSTimeSeriesDateInfo object defining the start date, end date and frequency of the timeseries.
    DateRange: A DSTimeSeriesDateRange object used to return the dates and values stored in the timeseries. See DSTimeSeriesDateRange for details.
    """
    def __init__(self, jsonDict = None, convertNoneToNans = False):
        super().__init__(jsonDict)
        self.DateInfo = None
        self.DateRange = None
        if jsonDict:
            self.DateInfo = DSTimeSeriesDateInfo(jsonDict['DateInfo'])
            self.DateRange = DSTimeSeriesDateRange(jsonDict['DateRange'], convertNoneToNans)


class DSTimeSeriesDateRangeResponse:
    """
    DSTimeSeriesDateRangeResponse is the object returned from the timeseries GetTimeseriesDateRange method. This method allows you to determine the 
    supported dates between given start and end dates at a specified frequency.

    Properties
    ----------
    Dates: A list of datetime values representing the supported dates between requested start and end dates at a specified frequency.
    ResponseStatus: This property will contain a DSUserObjectResponseStatus value. DSUserObjectResponseStatus.UserObjectSuccess represents a successful response.
    ErrorMessage: If ResponseStatus is not DSUserObjectResponseStatus.UserObjectSuccess this status string will provide a description of the error condition.
    Properties: Not currently used and will currently always return None.
    """
    def __init__(self, jsonDict = None):
        self.ResponseStatus = DSUserObjectResponseStatus.UserObjectSuccess
        self.ErrorMessage = ''
        self.Dates = None
        self.Properties = None
        if jsonDict:
            self.ResponseStatus = DSUserObjectResponseStatus(jsonDict['ResponseStatus'])
            self.ErrorMessage = jsonDict['ErrorMessage']
            # GetTimeseriesDateRange queries return a list of supported dates that fall between the specified start and end dates with the specified frequency
            if jsonDict['Dates']: # convert the json Dates to datetime
                self.Dates = jsonDict['Dates']
                for i in range(len(self.Dates)):
                    self.Dates[i] = DSUserObjectDateFuncs.jsonDateTime_to_datetime(self.Dates[i])
            self.Properties = jsonDict['Properties']


class TimeseriesClient(DSConnect):
    """
    TimeseriesClient is the client class that manages the connection to the API server on your behalf.
    It allows you to query for all your timeseries and to create/modify new timeseries.

    Methods Supported
    -----------------
    GetAllItems: Allows you to query for all the current timeseries available for your use
    GetItem: Allows you to download the details of a specific timeseries item.
    GetTimeseriesDateRange: Allows you to determine the supported timeseries dates between supplied start and end dates at a specified frequency.
    CreateItem: Allows you to create a new timeseries item with up to 130 years of daily data.
    UpdateItem: Allows you to update an existing timeseries.
    DeleteItem: Allows you to delete an existing timeseries.

    Note: You need a Datastream ID which is permissioned to access the Datastream APIs. 
          In addition, this ID also needs to be permissioned to access the custom user object service.
          Attempting to access this service without these permissions will result in a permission denied error response.

    Example usage:
    # first logon with your credentials. Creating a TimeseriesClient instance with your credentials automatically logs on for you 
    timeseriesClient = TimeseriesClient(None, 'YourID', 'YourPwd')
    # query for all your current timeseries items
    itemsResp = timeseriesClient.GetAllItems()
    if itemsResp:
        if itemsResp.ResponseStatus != DSUserObjectResponseStatus.UserObjectSuccess:
            # Your Datastream Id might not be permissioned for managing user created items on this API
            print('GetAllItems failed with error ' + itemsResp.ResponseStatus.name + ': ' + itemsResp.ErrorMessage)
        elif itemsResp.UserObjects and itemsResp.UserObjectsCount > 0:
            # You do have access to some timeseries

    """

    def __init__(self, config = None, username = None, password = None, proxies = None, sslVerify = None, sslCert = None):
        """ 
        Constructor: user details can be supplied from a config file or passed directly as parameters in the constructor of the derived user object type class.

        See the DSConnect superclass for a description of the connection parameters required.

        Timeseries Properties:
        ----------------------
        useNaNforNotANumber: Non-trading days are stored as double NaNs on Datastream but the JSON protocol doesn't permit NaNs as valid numbers.
        In comms with the API service, None (nulls) are used to represent NaNs in the JSON requests and responses.
        When creating or retrieving timeseries, None values are assumed as input and output values. If you want to supply and
        receive NaN float values, set useNaNforNotANumber to True on the client object. The client will then check input timeseries values for NaNs
        on Create/Update and convert to None. On responses (including GetItem), any Nones in the returned array of float values will be converted to NaNs.
        """
        super().__init__(config, username, password, proxies, sslVerify, sslCert)
        self.useNaNforNotANumber = False 

    def __JsonRequestEncoder(self, request):
        """
        We have to encode the timeseries request item with a type identifier to distinguish it properly as a timeseries request object for the API server.
        This method also converts the datetimes and values representin NaNs to a format acceptable in JSON queries.
        """
        jsonDict = dict()
        jsonDict.update({"__type":type(request).__name__ + self._apiSchemaNamespace})  # need to flag object type is a timeseries in JSON
        jsonDict.update(dict(request.__dict__))
        # DataInput object needs to be converted to dict so json.dumps() in DSConnect can convert the StartDate and EndDate to json dates
        # Also, if user has specified using NaNs rather than nulls, we need to convert any input NaNs to Nones for the JSON request to server
        if self.useNaNforNotANumber == True:
            temp = DSTimeSeriesDataInput(request.DataInput.StartDate, request.DataInput.EndDate, request.DataInput.Frequency)
            temp.Values = [None if val and math.isnan(val) else val for val in request.DataInput.Values]
            jsonDict['DataInput'] = temp.__dict__
        else:
            jsonDict['DataInput'] = request.DataInput.__dict__
        return jsonDict

    class __TimeseriesResponseType(IntEnum): # this private flag is used to indicate how the json response should be decoded into a response object.
        GetItemResponse = 0
        GetAllResponse = 1
        GetDateRangeResponse = 2

    def __as_getAllResponse(self, jsonDict):
        # An internal method to convert a JSON response from a GetAllItems query into a DSUserObjectGetAllResponse object.
        getAllResponse = DSUserObjectGetAllResponse(jsonDict)
        if ((jsonDict is not None) and (getAllResponse.UserObjects is not None) and (getAllResponse.ResponseStatus == DSUserObjectResponseStatus.UserObjectSuccess)
            and (getAllResponse.UserObjectType == DSUserObjectTypes.TimeSeries)):
            # convert all userobjects to DSTimeSeriesResponseObject
            for i in range(len(getAllResponse.UserObjects)):
                getAllResponse.UserObjects[i] = DSTimeSeriesResponseObject(getAllResponse.UserObjects[i], self.useNaNforNotANumber)
        return getAllResponse

    def __as_getResponse(self, jsonDict):
        # An internal method to convert a JSON response from GetItem, CreateItem or UpdateItem queries into a DSUserObjectResponse object.
        responseObject = DSUserObjectResponse(jsonDict)
        if ((jsonDict is not None) and (responseObject.UserObject is not None) and (responseObject.ResponseStatus == DSUserObjectResponseStatus.UserObjectSuccess)
            and (responseObject.UserObjectType == DSUserObjectTypes.TimeSeries)):
            responseObject.UserObject = DSTimeSeriesResponseObject(responseObject.UserObject, self.useNaNforNotANumber)
        return responseObject

    def __JsonResponseDecoder(self, jsonResp, responseType):
        # An internal method to convert a JSON response into the relevant DSUserObjectGetAllResponse, DSUserObjectResponse or DSTimeSeriesDateRangeResponse object.
        if responseType == self.__TimeseriesResponseType.GetAllResponse:
            return self.__as_getAllResponse(jsonResp)
        elif responseType == self.__TimeseriesResponseType.GetDateRangeResponse:
            return DSTimeSeriesDateRangeResponse(jsonResp)
        else: # GetItemResponse
            return self.__as_getResponse(jsonResp)

    def __CheckValidTimeseriesId(self, inputId):
        # The requested timeseries ID must match the format TS followed by 6 alphanumeric characters.
        if not isinstance(inputId, str) or not re.match("^TS[0-9A-Z]{6}$", inputId, re.IGNORECASE):
            return 'Timeseries IDs must be 8 uppercase alphanumeric characters in length and start with TS. e.g. TSABC001.'
        return None #valid

    def __CheckKeyTimeseriesProperties(self, tsItem):
        # must be a DSTimeSeriesRequestObject and have valid ID and DSTimeSeriesDataInput properties
        if not isinstance(tsItem, DSTimeSeriesRequestObject):
           return 'TimeseriesClient CreateItem or ModifyItem methods require a valid DSTimeSeriesRequestObject instance.'

        # must have a valid ID
        resp = self.__CheckValidTimeseriesId(tsItem.Id)
        if resp is not None:
            return resp

        # must have a valid DSTimeSeriesDataInput instance
        if not isinstance(tsItem.DataInput, DSTimeSeriesDataInput):
            return 'The supplied DSTimeSeriesRequestObject must supply a valid DSTimeSeriesDataInput instance.'
        # and valid date range
        if (not (isinstance(tsItem.DataInput.StartDate, date) or isinstance(tsItem.DataInput.StartDate, datetime))
                 or not (isinstance(tsItem.DataInput.EndDate, date) or isinstance(tsItem.DataInput.EndDate, datetime)) 
                 or (tsItem.DataInput.StartDate > tsItem.DataInput.EndDate)):
            return 'Supplied DSTimeSeriesDataInput StartDate and EndDate values must be date or datetime objects and StartDate cannot be set later then the EndDate.'
        # and a valid frequency
        if not isinstance(tsItem.DataInput.Frequency, DSUserObjectFrequency):
            return 'Supplied DSTimeSeriesDataInput Frequency field must be a DSUserObjectFrequency value.'
        # we must also have some values
        if (not tsItem.DataInput.Values) or len(tsItem.DataInput.Values) == 0:
            return 'Supplied DSTimeSeriesDataInput Values field must contain an array of values.'

        # some safety checks
        # Mnemonic isn't used in timeseries; should be the same as ID 
        tsItem.Mnemonic = tsItem.Id
        tsItem.ManagementGroup = tsItem.ManagementGroup if isinstance(tsItem.ManagementGroup, str) else None
        tsItem.Units = tsItem.Units if isinstance(tsItem.Units, str) else None
        tsItem.DecimalPlaces = tsItem.DecimalPlaces if isinstance(tsItem.DecimalPlaces, int) and tsItem.DecimalPlaces >= 0 and tsItem.DecimalPlaces <= 8 else 0
        tsItem.AsPercentage = tsItem.AsPercentage if isinstance(tsItem.AsPercentage, bool) else False
        tsItem.FrequencyConversion = tsItem.FrequencyConversion if isinstance(tsItem.FrequencyConversion, DSTimeSeriesFrequencyConversion) else DSTimeSeriesFrequencyConversion.EndValue
        tsItem.DateAlignment = tsItem.DateAlignment if isinstance(tsItem.DateAlignment, DSTimeSeriesDateAlignment) else DSTimeSeriesDateAlignment.EndPeriod
        tsItem.CarryIndicator = tsItem.CarryIndicator if isinstance(tsItem.CarryIndicator, DSTimeSeriesCarryIndicator) else DSTimeSeriesCarryIndicator.Yes
        tsItem.PrimeCurrencyCode = tsItem.PrimeCurrencyCode if isinstance(tsItem.PrimeCurrencyCode, str) else None
        # Redundant properties
        tsItem.UnderCurrencyCode = None # Deprecated.
        tsItem.HasPadding = False # Deprecated and replaced with CarryIndicator.
        tsItem.AsPercentage = False # Deprecated.
        # We should ensure some safety values for base class for JSON encoding purposes in case default values overwritten with incorrect types
        tsItem.SetSafeUpdateParams()
        return None  # valid request



    def GetAllItems(self):
        """ GetAllItems returns all the current timeseries you can use in Datastream queries.
  
            Example usage:
            # first logon with your credentials. Creating a TimeseriesClient instance with your credentials automatically logs on for you 
            timeseriesClient = TimeseriesClient(None, 'YourID', 'YourPwd')
            # query for all your timeseries
            itemsResp = timeseriesClient.GetAllItems()
            # DSUserObjectGetAllResponse has a ResponseStatus property that indicates success or failure for the query
            if itemsResp:
                if itemsResp.ResponseStatus != DSUserObjectResponseStatus.UserObjectSuccess:
                    # Your Datastream Id might not be permissioned for managing user created items on this API
                    print('GetAllItems failed with error ' + itemsResp.ResponseStatus.name + ': ' + itemsResp.ErrorMessage, end='\n\n')
                elif itemsResp.UserObjectsCount == 0 or itemsResp.UserObjects == None:
                    print('GetAllItems returned zero timeseries items.', end='\n\n')
                else:
                    # You do have access to some timeseries
                    # Here we just put the timeseries details into a dataframe and list them
                    print('{}{}{}'.format('GetAllItems returned ', itemsResp.UserObjectsCount, ' timeseries items.'))
                    data  = []
                    colnames = ['Id', 'Desc', 'LastModified', 'StartDate', 'EndDate', 'Frequency', 'NoOfValues']
                    for tsItem in itemsResp.UserObjects:
                        if tsItem:
                            rowdata = [tsItem.Id, tsItem.Description, tsItem.LastModified, tsItem.DateInfo.StartDate if tsItem.DateInfo else None, tsItem.DateInfo.EndDate if tsItem.DateInfo else None,
                                        tsItem.DateInfo.Frequency if tsItem.DateInfo else None, tsItem.DateRange.ValuesCount if tsItem.DateRange else 0]
                            data.append(rowdata)
                    df = pd.DataFrame(data, columns=colnames)
                    print(df, end='\n\n')
        """
        try:
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.GetAllItems', 'GetAllItems requested')
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            # construct the request
            request_url = self.url + 'GetAllItems'
            raw_request = {"Filters" : None,
                           "Properties" : None,
                           "TokenValue" : self.token,
                           "UserObjectType" : DSUserObjectTypes.TimeSeries}

            # make the request and process the response
            json_Response = self._get_json_Response(request_url, raw_request)
            decoded = self.__JsonResponseDecoder(json_Response, self.__TimeseriesResponseType.GetAllResponse)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.GetAllItems', 'GetAllItems returned')
            return decoded
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'TimeseriesClient.GetAllItems', 'Exception occured.', exp)
            raise exp


    def GetItem(self, itemId):
        """ GetItem returns the details for an individual timeseries.
            Parameters: a valid timeseries Id.

            Example usage:
            # first logon with your credentials. Creating a TimeseriesClient instance with your credentials automatically logs on for you 
            timeseriesClient = TimeseriesClient(None, 'YourID', 'YourPwd')
            # query for a specific timeseries
            tsName = 'TSZZZ001'
            tsResponse = timeseriesClient.GetItem(tsName)

            # You may want to put the timeseries request response handling into a common function.
            if tsResponse:
                # Any request dealing with a single user created item returns a DSUserObjectResponse. This has ResponseStatus property that indicates success or failure
                if tsResponse.ResponseStatus != DSUserObjectResponseStatus.UserObjectSuccess:
                    print('Request failed for timeseries ' + tsName + ' with error ' + tsResponse.ResponseStatus.name + ': ' + tsResponse.ErrorMessage, end='\n\n')
                elif tsResponse.UserObject != None:  # The timeseries item won't be returned if you set SkipItem true in CreateItem or UpdateItem
                    # Here we simply display the timeseries data using a dataframe.
                    tsItem = tsResponse.UserObject
                    names = ['Id', 'Desc', 'LastModified', 'StartDate', 'EndDate', 'Frequency', 'NoOfValues']
                    coldata = [tsItem.Id, tsItem.Description, tsItem.LastModified, tsItem.DateInfo.StartDate if tsItem.DateInfo else None, tsItem.DateInfo.EndDate if tsItem.DateInfo else None,
                                      tsItem.DateInfo.Frequency if tsItem.DateInfo else '', tsItem.DateRange.ValuesCount if tsItem.DateRange else 0]
                    if tsItem.DateRange:
                        names = names + tsItem.DateRange.Dates
                        coldata = coldata +tsItem.DateRange.Values
                    df = pd.DataFrame(coldata, index=names)
                    print(df, end='\n\n')
                else:
                    print('Timeseries  ' + tsName + ' successfully updated but item details not returned.')
       """
        try:
            # Some prechecks
            reqCheck = self.__CheckValidTimeseriesId(itemId)
            if reqCheck is not None:
                resp = DSUserObjectResponse()
                resp.ResponseStatus = DSUserObjectResponseStatus.UserObjectFormatError
                resp.ErrorMessage = reqCheck
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'TimeseriesClient.GetItem', 'Error: ' + reqCheck)
                return resp
            
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.GetItem', 'Requesting ' + itemId)
            self.Check_Token() # check and renew token if within 15 minutes of expiry
 
            # construct the request
            request_url = self.url + 'GetItem'
            raw_request = { "Filters" : None,
                            "Properties" : None,
                            "TokenValue" : self.token,
                            "UserObjectId" : itemId,
                            "UserObjectType" : DSUserObjectTypes.TimeSeries }

            # make the request and process the response
            json_Response = self._get_json_Response(request_url, raw_request)
            decoded = self.__JsonResponseDecoder(json_Response, self.__TimeseriesResponseType.GetItemResponse)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.GetItem', itemId + ' returned a response')
            return decoded
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'TimeseriesClient.GetItem', 'Exception occured.', exp)
            raise exp


    def GetTimeseriesDateRange(self, startDate, endDate, frequency = DSUserObjectFrequency.Daily):
        """ GetTimeseriesDateRange: This method allows you to determine the supported dates between supplied start and end dates at a specified frequency.
            Parameters:
                startDate: A date specifying the beginning of the date range
                endDate: A date specifying the end of the date range
                frequency: A DSUserObjectFrequency enumeration defining if the frequency should be daily, weekly, monthly, quarterly or yearly.
            Notes:
                For Daily and Weekly frequencies, if the supplied startDate falls on a weekend or a trading holiday, the returned starting date will be the first 
                trading day before the given start date. If the supplied endDate falls on a weekend or a trading holiday, the returned final date will be the last trading
                day before the given end date. For Weekly frequencies, this will be the last date which matches the day of the week for the first returned start date.

                For Monthly, Quarterly and Yearly frequencies, the returned dates are always the 1st day of each month, quarter or year. The returned start and end dates
                are always the 1st days of the requested month, quarter or year that the given start and end dates fall within.

            Example usage:
            # first logon with your credentials. Creating a TimeseriesClient instance with your credentials automatically logs on for you 
            timeseriesClient = TimeseriesClient(None, 'YourID', 'YourPwd')

            # query for a specific range of dates
            startDate = date(2016, 1, 1)
            endDate = date.fromisoformat('2022-04-01')
            freq = DSUserObjectFrequency.Quarterly
            dateRangeResp = timeseriesClient.GetTimeseriesDateRange(startDate, endDate, freq)

            #process the returned dates
            if dateRangeResp:
                if dateRangeResp.ResponseStatus != DSUserObjectResponseStatus.UserObjectSuccess:
                    print('GetTimeseriesDateRange failed with error ' + dateRangeResp.ResponseStatus.name + ': ' + dateRangeResp.ErrorMessage)
                elif dateRangeResp.Dates != None:
                    df = pd.DataFrame(dateRangeResp.Dates, index = None)
                    print(df, end='\n\n')
        """
        try:
            #Check startDate is before endDate
            reqCheck = None
            if (not isinstance(startDate, date)) or (not isinstance(endDate, date)) or (startDate > endDate):
                reqCheck = 'Supplied StartDate and EndDate parameters must be date objects and StartDate cannot be set later then the EndDate.'
            elif not isinstance(frequency, DSUserObjectFrequency):
                return 'Supplied frequency parameter must be a DSUserObjectFrequency value.'
            if reqCheck is not None:
                resp = DSTimeSeriesDateRangeResponse()
                resp.ResponseStatus = DSUserObjectResponseStatus.UserObjectFormatError
                resp.ErrorMessage = reqCheck
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'TimeseriesClient.GetTimeseriesDateRange', 'Error: ' + resp.ErrorMessage)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.GetTimeseriesDateRange', 'Requesting date range')
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            # construct our DSTimeSeriesDateInfo object
            dateInfo =  DSTimeSeriesDateInfo(None)
            dateInfo.StartDate = startDate
            dateInfo.EndDate = endDate
            dateInfo.Frequency = frequency

            # construct the request
            request_url = self.url + "TimeSeriesGetDateRange"
            raw_request = { "DateInfo" : dateInfo.__dict__,
                            "Properties" : None,
                            "TokenValue" : self.token}

            # make the request and process the response
            json_Response = self._get_json_Response(request_url, raw_request)
            decoded = self.__JsonResponseDecoder(json_Response, self.__TimeseriesResponseType.GetDateRangeResponse)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.GetTimeseriesDateRange', 'GetTimeseriesDateRange returned a response')
            return decoded
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'TimeseriesClient.GetTimeseriesDateRange', 'Exception occured.', exp)
            raise exp


    def CreateItem(self, newItem, overWrite = False, skipItemReturn = False):
        """ CreateItem: This method attempts to create the given DSTimeSeriesRequestObject via the API service
            Parameters:
                newItem: A DSTimeSeriesRequestObject containing the data used for creating the Timeseries.
                overWrite: If the given Timeseries Id already exists on the system, the create call will be rejected. Set overWrite = True to overwrite the existing item with new Timeseries.
                skipItemReturn: Upon successful creation of an item, the server requests the new item from the mainframe and returns it in the response object.
                            For faster processing, set skipItemReturn = True to skip returning the object in the response (DSUserObjectResponse.UserObject = None)
            Notes:
                For Daily and Weekly frequencies, if the supplied startDate falls on a weekend or a trading holiday, the returned starting date will be the first 
                trading day before the given start date. If the supplied endDate falls on a weekend or a trading holiday, the returned final date will be the last trading
                day before the given end date. For Weekly frequencies, this will be the last date which matches the day of the week for the first returned start date.

                For Monthly, Quarterly and Yearly frequencies, the returned dates are always the 1st day of each month, quarter or year. The returned start and end dates
                are always the 1st days of the requested month, quarter or year that the given start and end dates fall within.

            Example usage:
            # first logon with your credentials. Creating a TimeseriesClient instance with your credentials automatically logs on for you 
            timeseriesClient = TimeseriesClient(None, 'YourID', 'YourPwd')

            # create your timeseries request object. You would normally use the list of supported dates from a GetTimeseriesDateRange query.
            startDate = date(2016, 1, 1)
            endDate = date.fromisoformat('2022-04-01')
            freq = DSUserObjectFrequency.Quarterly
            # For this example, we'll just poulate an array of test values randomly between 10.00 and 200.00 with two decimal places
            datesCount = 26    
            random.seed()
            # We'll simply create an array of values datesCount long with random values between 10 and 200 with 2 decimal places to reflect data retrieved from some source
            values = [(random.randint(1000, 20000) / 100) for k in range(0, datesCount)] 

            # First method is to use the optional parameters in the constructor to supply the key data
            testTs = DSTimeSeriesRequestObject('TSZZZ001', startDate, endDate, DSUserObjectFrequency.Quarterly, values)

            # Create the item
            tsResp = timeseriesClient.CreateItem(testTs)
            # Process the response
            if tsResponse:
                # see the GetItem method for an example of handling a timeseries response
        """
        try:
            # Some prechecks
            reqCheck = self.__CheckKeyTimeseriesProperties(newItem)
            if reqCheck is not None:
                resp = DSUserObjectResponse()
                resp.ResponseStatus = DSUserObjectResponseStatus.UserObjectFormatError
                resp.ErrorMessage = reqCheck
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'TimeseriesClient.CreateItem', 'Error: ' + reqCheck)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.CreateItem', 'Creating ' + newItem.Id)
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            # encode the DSTimeSeriesRequestObject into JSON
            request_url = self.url + "CreateItem"
            jsonReq = self.__JsonRequestEncoder(newItem)

            # we may need to encode Filters properties with flags to overwrite item if it already exists, plus option not to return the timeseries in the response
            filters = None
            if overWrite == True or skipItemReturn == True:
                filters = []
                if overWrite == True:
                    filters.append({"Key": "ForceUpdate", "Value": True})
                if skipItemReturn == True:
                    filters.append({"Key": "SkipRetrieval", "Value": True})

            # construct the raw request and make the Rest/JSON query
            raw_request = { "Filters" : filters,
                            "Properties" : None,
                            "TokenValue" : self.token,
                            "UserObject" : jsonReq,
                            "UserObjectType" : DSUserObjectTypes.TimeSeries}

            # make the request and process the response
            json_Response = self._get_json_Response(request_url, raw_request)
            decoded = self.__JsonResponseDecoder(json_Response, self.__TimeseriesResponseType.GetItemResponse)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.CreateItem', newItem.Id + ' returned a response')
            return decoded
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'TimeseriesClient.CreateItem', 'Exception occured.', exp)
            raise exp


    def UpdateItem(self, item, skipItemReturn = False):
        """ UpdateItem: This method attempts to modify a timeseries item using the given DSTimeSeriesRequestObject via the API service
            Parameters:
                newItem: A DSTimeSeriesRequestObject containing the data used for creating the Timeseries.
                skipItemReturn: Upon successful creation of an item, the server requests the new item from the mainframe and returns it in the response object.
                            For faster processing, set skipItemReturn = True to skip returning the object in the response (DSUserObjectResponse.UserObject = None)

            Example usage:
            # first logon with your credentials. Creating a TimeseriesClient instance with your credentials automatically logs on for you 
            timeseriesClient = TimeseriesClient(None, 'YourID', 'YourPwd')

            # create your timeseries request object. You would normally use the list of supported dates from a GetTimeseriesDateRange query.
            startDate = date(2016, 1, 1)
            endDate = date.fromisoformat('2022-04-01')
            freq = DSUserObjectFrequency.Quarterly
            # For this example, we'll just poulate an array of test values randomly between 10.00 and 200.00 with two decimal places
            datesCount = 26    
            random.seed()
            # We'll simply create an array of values datesCount long with random values between 10 and 200 with 2 decimal places to reflect data retrieved from some source
            values = [(random.randint(1000, 20000) / 100) for k in range(0, datesCount)] 

            # First method is to use the optional parameters in the constructor to supply the key data
            testTs = DSTimeSeriesRequestObject('TSZZZ001', startDate, endDate, DSUserObjectFrequency.Quarterly, values)

            # Update the item
            tsResp = timeseriesClient.UpdateItem(testTs)
            # Process the response
            if tsResponse:
                # see the GetItem method for an example of handling a timeseries response
        """
        try:
            # Some prechecks
            reqCheck = self. __CheckKeyTimeseriesProperties(item)
            if reqCheck is not None:
                resp = DSUserObjectResponse()
                resp.ResponseStatus = DSUserObjectResponseStatus.UserObjectFormatError
                resp.ErrorMessage = reqCheck
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'TimeseriesClient.UpdateItem', 'Error: ' + reqCheck)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.UpdateItem', 'Updating ' + item.Id)
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            # encode the DSTimeSeriesRequestObject into JSON
            request_url = self.url + 'UpdateItem'
            jsonReq = self.__JsonRequestEncoder(item)

            # construct the raw request and make the Rest/JSON query
            # we may need to encode Filters properties with option not to return the timeseries in the response
            raw_request = { "Filters" : [ { "Key": "SkipRetrieval", "Value": True} ] if skipItemReturn == True else None,
                            "Properties" : None,
                            "TokenValue" : self.token,
                            "UserObject" : jsonReq,
                            "UserObjectType" : DSUserObjectTypes.TimeSeries}

            # make the request and process the response
            json_Response = self._get_json_Response(request_url, raw_request)
            decoded = self.__JsonResponseDecoder(json_Response, self.__TimeseriesResponseType.GetItemResponse)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.UpdateItem', item.Id + ' returned a response')
            return decoded
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'TimeseriesClient.UpdateItem', 'Exception occured.', exp)
            raise exp

    def DeleteItem(self, itemId):
        """ DeleteItem allows you to delete an existing timeseries
            Parameters: a valid timeseries Id.

            Example usage:
            # first logon with your credentials. Creating a TimeseriesClient instance with your credentials automatically logs on for you 
            timeseriesClient = TimeseriesClient(None, 'YourID', 'YourPwd')

            # Deleting an item is simply a matter of supplying the timeseries ID.
            delResp = timeseriesClient.DeleteItem('TSZZZ001')
            if delResp:
                if delResp.ResponseStatus != DSUserObjectResponseStatus.UserObjectSuccess:
                    print('Timeseries DeleteItem failed on ' + delResp.UserObjectId + ' with error ' + delResp.ResponseStatus.name + ': ' + delResp.ErrorMessage, end='\n\n')
                else:
                    print('Timeseries  ' + delResp.UserObjectId + ' successfully deleted.', end='\n\n')
        """
        try:
            # Some prechecks
            reqCheck = self.__CheckValidTimeseriesId(itemId)
            if reqCheck is not None:
                resp = DSUserObjectResponse()
                resp.ResponseStatus = DSUserObjectResponseStatus.UserObjectFormatError
                resp.ErrorMessage = reqCheck
                DSUserObjectLogFuncs.LogError('DatastreamPy', 'TimeseriesClient.DeleteItem', 'Error: ' + reqCheck)
                return resp

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.DeleteItem', 'Deleting ' + itemId)
            self.Check_Token() # check and renew token if within 15 minutes of expiry

            # construct the request
            request_url = self.url + 'DeleteItem'
            raw_request = { "Filters" : None,
                            "Properties" : None,
                            "TokenValue" : self.token,
                            "UserObjectId" : itemId,
                            "UserObjectType" : DSUserObjectTypes.TimeSeries}

            # make the request and process the response
            json_Response = self._get_json_Response(request_url, raw_request)
            decoded = self.__JsonResponseDecoder(json_Response, self.__TimeseriesResponseType.GetItemResponse)
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'TimeseriesClient.DeleteItem', itemId + ' returned a response')
            return decoded
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'TimeseriesClient.DeleteItem', 'Exception occured.', exp)
            raise exp

    

