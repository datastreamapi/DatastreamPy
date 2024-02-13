"""
Datastream User Created Items
-----------------------------

Datastream permits users to create and manage custom items. Five user created item types are supported:

    Timeseries - Upload your own timeseries data to Datastream’s central systems. These can then be used in combination with Datastream maintained series in charts 
                    and data requests with the full power of the Datastream expressions language.
    Lists - Create and manage your own lists of constituent items. These can then be used to request static data for the constituents of the lists.
    Expressions - Combine Datastream data items with analytical functions to create custom expressions.
    Indices - Create an index based on a portfolio of instruments and and measure the performance against variations on established market indices and other benchmarks.
    Regression - Create a regression model to analyse the extent to which one time series (the dependent variable) is influenced by other time series.

Each of these five user created types have their own object model. However, some parameters, such as the last modified time (LastModified), are common to all five 
types. This module defines the DSUserObjectBase class common to all types plus the supporting objects and enums that permit clients to manage their user created items.

This module also contains some basic datetime conversions to and from JSON plus some basic logging functionality common to the objects
"""

from enum import IntEnum
import pytz
import json
import re
from datetime import datetime, timedelta, date

class DSPackageInfo:
    buildVer = '2.0.21'
    appId = 'DatastreamPy-' + buildVer
    UserAgent = ' DatastreamPy/' + buildVer

class DSUserObjectFault(Exception):
    """
    DSUserObjectFault exception is a representation of the DSFault error returned from the API server.
    DSFaults are returned for the following reasons:
        Invalid credentials
        Empty credentials
        Access blocked due to missuse of the service.

    A DSUserObjectFault will be thrown in the event a DSFault is returned from the server

    Note: Once access is granted, any other errors, such as not being explicity permissioned to manage user created items, or requesting an invalid object
    returns a DSUserObjectResponseStatus error flag (e.g. DSUserObjectResponseStatus.UserObjectPermissions) in the ResponseStatus property of the returned 
    DSUserObjectResponse or DSUserObjectGetAllResponse object. The response's ErrorMessage property will specify the error condition
    """
    def __init__(self, jsonDict):
        super().__init__(jsonDict['Message'])
        self.Code = jsonDict['Code']
        self.SubCode = jsonDict['SubCode']


class DSUserObjectLogLevel(IntEnum):
    """
    The module does some basic logging and this enumeration determines the level of logging to report
    LogNone - turn logging off completely
    LogError - log only errors such as exceptions, etc.
    LogWarning - log non-fatal errors
    LogInfo - log major steps such as starting API query and receiving response
    LogTrace - log minor steps such as converting object to JSON, etc.
    LogVerbose -log the full json request content, etc.

    Used in DSUserObjectLogFuncs methods
    Example:
    DSUserObjectLogFuncs.LogLevel = DSUserObjectLogFuncs.LogError

    """
    LogNone = 0
    LogError = 1
    LogWarning = 2
    LogInfo = 3
    LogTrace = 4
    LogVerbose = 5


class DSUserObjectLogFuncs:
    """
    DSUserObjectLogFuncs is used to log actions within the user created client classes (e.g. CreateItem, UpdateItem, etc)

    The API methods call methods here to log processing steps.
    Methods:
    LogException - used to log exceptions returned due to network failure, invalid credentials, etc
    LogError - used to log general error messages
    LogDetail - used to log info with a specified logging level

    These methods call methods LogExcepFunc, LogErrorFunc and LogDetailFunc which can be overridden to implement your own logging.
    By default, LogExcepFunc, LogErrorFunc and LogDetailFunc call internal functions that just call print function

    An example of overriding the implementation:
    def LogDetailOverride(loglevel, moduleName, funcName, commentStr, verboseObj = None):
        # provide override implementation
        print('....')

    #use in built logging with print function
    DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'X', 'Y', 'Z')
    #use custom function LogDetailOverride
    DSUserObjectLogFuncs.LogDetailFunc = LogDetailOverride
    DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'X', 'Y', 'Z')

    """
    LogLevel = DSUserObjectLogLevel.LogNone  # By default we assume no logging required

    # internal functions for logging that can be overridden. default is basic print method
    @staticmethod
    def __logExcepInternal(moduleName, funcName, commentStr, excep):
        # internal default function for basic logging of exceptions using print method
        print(str(datetime.utcnow()), moduleName, funcName, commentStr, sep=': ')
        print(excep)

    @staticmethod
    def __logErrorInternal(moduleName, funcName, commentStr, verboseObj):
        # internal default function for basic logging of errors using print method
        print(str(datetime.utcnow()), moduleName, funcName, commentStr, sep=': ')
        if verboseObj:
            print(verboseObj)

    @staticmethod
    def __logDetailInternal(loglevel, moduleName, funcName, commentStr, verboseObj):
        # internal default function for basic logging of generic logs using print method
        print(str(datetime.utcnow()), moduleName, funcName, commentStr, sep=': ')
        if verboseObj and DSUserObjectLogFuncs.LogLevel >= DSUserObjectLogLevel.LogVerbose:
            print(verboseObj)

    # function overrides to allow you to redirect the logging to custom handlers. They default to the internal static methods that just perform printing
    LogExcepFunc, LogErrorFunc, LogDetailFunc = __logExcepInternal, __logErrorInternal, __logDetailInternal 

    # the public logging functions used by the user created classes
    @staticmethod
    def LogException(moduleName, funcName, commentStr, excep):
        # Used to log exceptions returned due to network failure, invalid credentials, etc
        if DSUserObjectLogFuncs.LogLevel >= DSUserObjectLogLevel.LogError:
            DSUserObjectLogFuncs.LogExcepFunc(moduleName, funcName, commentStr, excep)

    @staticmethod
    def LogError(moduleName, funcName, commentStr, verboseObj = None):
        # Used to log general error messages
        if DSUserObjectLogFuncs.LogLevel >= DSUserObjectLogLevel.LogError:
            DSUserObjectLogFuncs.LogErrorFunc(moduleName, funcName, commentStr, verboseObj)

    @staticmethod
    def LogDetail(loglevel, moduleName, funcName, commentStr, verboseObj = None):
        # Used to log info with a specified logging level
        if DSUserObjectLogFuncs.LogLevel >= loglevel:
            DSUserObjectLogFuncs.LogDetailFunc(loglevel, moduleName, funcName, commentStr, verboseObj)


class DSUserObjectDateFuncs:
    """
    DSUserObjectDateFuncs is used internally to convert datetimes to and from JSON "/Date()" format for comms with the API server
    """
    __epoch_date = datetime(1970, 1, 1, tzinfo=pytz.utc)

    @staticmethod
    def jsonDateTime_to_datetime(jsonDate):
        # Convert a JSON /Date() string to a datetime object
        if jsonDate is None:
            return None
        try:
            match = re.match(r"^(/Date\()(-?\d*)(\)/)", jsonDate)
            if match == None:
                match = re.match(r"^(/Date\()(-?\d*)([+-])(..)(..)(\)/)", jsonDate)
            if match:
                return DSUserObjectDateFuncs.__epoch_date + timedelta(seconds=float(match.group(2))/1000)
            else:
                raise Exception("Invalid JSON Date: " + jsonDate)
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DSUserDataObjectBase.py', 'DSUserObjectDateFuncs.jsonDateTime_to_datetime', 'Exception occured:', exp)
            raise

    @staticmethod
    def toJSONdate(inputDate):
        # convert to /Date() object with no of ticks relative to __epoch_date
        if isinstance(inputDate, datetime) or isinstance(inputDate, date):
            inputDate = datetime(inputDate.year, inputDate.month, inputDate.day, tzinfo=pytz.utc)
        elif isinstance(inputDate, str):
            inputDate = datetime.strptime(inputDate, "%Y-%m-%d")
            inputDate = datetime(inputDate.year, inputDate.month, inputDate.day, tzinfo=pytz.utc)
        else: #should not call with unsupported type
            return inputDate
        ticks = (inputDate - DSUserObjectDateFuncs.__epoch_date).total_seconds() * 1000
        jsonTicks = "/Date(" + str(int(ticks)) + ")/"
        return jsonTicks


class DSUserObjectTypes(IntEnum):
    """
    Five user created types are supported. When the client classes communicate with the API server, a DSUserObjectTypes property is set to specify the object type.
    Responses from the API server also specify the type of the object being returned.
    """
    NoType = 0 # if your request fails, returning no user created item, the response object's UserObjectType field will be set to this value
    List = 1
    Index = 2
    TimeSeries = 3
    Expression = 4
    Regression = 5  

    
class DSUserObjectResponseStatus(IntEnum):
    """
    All client methods to retrieve or modify user created items return a respone object which includes a ResponseStatus property.
    The ResponseStatus property specifies success or failure for the request using a DSUserObjectResponseStatus value

    Response Values:
        UserObjectSuccess: The request succeeded and the response object's UserObject(s) property should contain the (updated) object (except for DeleteItem method).
        UserObjectPermissions: Users need to be specifically permissioned to create custom objects. This flag is set if you are not currently permissioned.
        UserObjectNotPresent: Returned if the requested ID does not exist.
        UserObjectFormatError: Returned if your request object is not in the correct format.
        UserObjectTypeError: Returned if your supplied object is not the same as the type specified.
        UserObjectError:  The generic error flag. This will be set for any error not specified above. Examples are:
            Requested object ID is not present
            You have exceeded the number of custom objects permitted on your account.
    """
    UserObjectSuccess = 0
    UserObjectPermissions = 1
    UserObjectNotPresent = 2
    UserObjectFormatError = 3
    UserObjectTypeError = 4
    UserObjectError = 5


class DSUserObjectFrequency(IntEnum):
    """
    Regressions and Timeseries objects specify a frequency for the underlying data. DSUserObjectFrequency defines the supported frequencies.
    """
    Daily = 0
    Weekly = 1
    Monthly = 2
    Quarterly = 3
    Yearly = 4


class DSUserObjectShareTypes(IntEnum):
    """
    All user created objects have a flag specifying how they are shared with other users. Currently only PrivateUserGroup and Global are supported.

    For all object types other than expressions and indices, the share type is always PrivateUserGroup. PrivateUserGroup are items created by any 
    Datastream ID that shares a parent Datastream ID with your ID. Only children of the parent ID can access the user object.

    Expressions can be either PrivateUserGroup or Global. Like the other object types, PrivateUserGroup items are items created by users and visible to
    just children of their Datastream parent ID. PrivateUserGroup expressions have the ID signature Eaaa, where 'a' is any alphabetical character.
    
    Global expressions are Datastream owned expressions that are available for use by any client. These have the signature nnnE, where n is a digit. 
    Global expressions can be retrieved using the API service, but you cannot modify them. Only your PrivateUserGroup items can be modified.

    Indices can be either PrivateUserGroup or UserGroup. Like the other object types, PrivateUserGroup items are items created by users and visible to
    just children of their Datastream parent ID.

    Authorized Indices are special, restricted class of indices that can be shared across parent IDs. This is a special case for companies that have 
    more than one parent ID. With Datastream's permission, indices can be marked as authorized and shared across a restricted ste of parent IDs. In
    these circumstances, authorized indices are marked as UserGroup.
    """
    NoType = 0
    Company = 1 # not currently supported
    PrivateUserGroup = 2
    UserGroup = 3
    Global = 4


class DSUserObjectAccessRights(IntEnum):
    """
    All user created objects have a flag specifying if they can be modified by the user.

    All items that have their ShareType property set to DSUserObjectShareTypes. PrivateUserGroup will also have their AccessRight property set to 
    ReadWrite. Global expression objects, not being editable by users, will have the AccessRight property set to Read.
    """
    ReadWrite = 0
    Read = 1


class DSUserObjectBase:
    """
    DSUserObjectBase is the base object for all five user object types. It defines the properties common to all the types

    Properties
    ----------
    ID: The object identifier. The format is specific to each object type. See the individual object file for the particular specification
    Mnemonic: For all object types bar indices, this is the same as the Id property. For indices, the ID (of the form X#:Xnnnnn where n is a digit) is
                returned when you create an index and is used to manage the index via the API interface. The Mnemonic property is specified when creating
                an index and is used to reference the index when using Datastream tools such as Charting, Datastream For Office, etc. 
                A mnemonic has format X#aaaaa where aaaaa is 1 to 6 alphanumeric characters.
    DisplayName: A string describing the object. The maximum length varies from object type to object type:
                Expression: Max 30 alphanumeric characters.
                Index: Max 60 alphanumeric characters.
                List: Max 60 alphanumeric characters.
                Regression: Max 50 alphanumeric characters.
                Timeseries: Max 64 alphanumeric characters.
    Description: Currently this isn't supported. When the API returns an object, the Description property will be the same as the DisplayName property.
    Created: a datetime value representing the date when the object was first created.
    LastModified: a datetime value representing the date when the object was last updated.
    Owner: The parent Datastream ID that owns the object. This will be the parent of your Datastream ID. For global expressions this will always be 'Admin'
    ShareType: For all objects except global expressions, this will be DSUserObjectShareTypes.PrivateUserGroup. For global expressions it will be DSUserObjectShareTypes.Global.
    AccessRight: For all objects except global expressions, this will be DSUserObjectAccessRights.ReadWrite. For global expressions it will be DSUserObjectAccessRights.Read.
    """
    def __init__(self, jsonDict):
        self.Id = None
        self.Mnemonic = None
        self.DisplayName = None
        self.Description = None
        self.Created = datetime.utcnow()  # only valid when received as a response. On create or update this field is ignored
        self.LastModified = datetime.utcnow() # only valid when received as a response. On create or update this field is ignored
        self.Owner = None   # only valid when received as a response. On create or update this field is ignored
        self.ShareType = DSUserObjectShareTypes.PrivateUserGroup # all items except reserved global expressions (available to all clients) are PrivateUserGroup
        self.AccessRight = DSUserObjectAccessRights.ReadWrite # all items except reserved global expressions (available to all clients) are ReadWrite
        if jsonDict: # upon a successful response from the API server jsonDict will be used to populate the DSUserObjectBase object with the response data.
            self.Id = jsonDict['Id']
            self.Mnemonic = jsonDict['Mnemonic']
            self.DisplayName = jsonDict['DisplayName']
            self.Description = jsonDict['Description']
            self.Created = DSUserObjectDateFuncs.jsonDateTime_to_datetime(jsonDict['Created'])
            self.LastModified = DSUserObjectDateFuncs.jsonDateTime_to_datetime(jsonDict['LastModified'])
            self.Owner = jsonDict['Owner']
            self.ShareType = DSUserObjectShareTypes(jsonDict['ShareType'])
            self.AccessRight = DSUserObjectAccessRights(jsonDict['AccessRight'])

    def SetSafeUpdateParams(self):
        """ SetSafeUpdateParams: The following parameters are set only in response when we query for user created items. 
        This method is called before Create or Update to ensure safe values set prior to JSON encoding"""
        self.Created = datetime.utcnow()  # only valid when received as a response. On create or update this field is ignored
        self.LastModified = datetime.utcnow() # only valid when received as a response. On create or update this field is ignored
        self.Owner = None   # only valid when received as a response. On create or update this field is ignored
        self.ShareType = DSUserObjectShareTypes.PrivateUserGroup # all items except reserved global expressions (available to all clients) are PrivateUserGroup
        self.AccessRight = DSUserObjectAccessRights.ReadWrite # all items except reserved global expressions (available to all clients) are ReadWrite
        # Description is not currently implemented, so ensure safety as string if set
        self.Description = self.Description if isinstance(self.Description, str) else None
        self.DisplayName = self.DisplayName if isinstance(self.DisplayName, str) else None


class DSUserObjectGetAllResponse:
    """
    DSUserObjectGetAllResponse is the object returned for the client class' GetAllItems query only.

    Properties
    ----------
    UserObjectType: specifies the returned object types. e.g. DSUserObjectTypes.List, DSUserObjectTypes.TimeSeries, etc.
    UserObjects: An array of the specified object types such as DSListUserObject, DSRegressionUserObject, etc.
    UserObjectsCount: The number of objects returned in the UserObjects property.
    ResponseStatus: This property will contain a DSUserObjectResponseStatus value. DSUserObjectResponseStatus.UserObjectSuccess represents a successful response.
    ErrorMessage: If ResponseStatus is not DSUserObjectResponseStatus.UserObjectSuccess this status string will provide a description of the error condition.
    Properties: Not currently used and will currently always return None.

    Note: For GetAllItems queries only, the returned objects will not have all their propety fields set. Specifically:
        Expression: All property fields are fully populated.
        Index: The ConstituentsCount property will correctly specify the number of constituents but the Constituents property will be None.
        List: The ConstituentsCount property will correctly specify the number of constituents but the Constituents property will be None.
        Regression: All property fields are fully populated.
        Timeseries: The ValuesCount field of the DateRange property will specify the number of date value pairs, but the Dates and Values fields will be None.
    
    You need to query for the individual object using the GetItem request to retrieve the full content for the object.

    """
    def __init__(self, jsonDict = None):
        self.UserObjectType = DSUserObjectTypes.NoType
        self.UserObjects = None
        self.UserObjectsCount = 0
        self.ResponseStatus = DSUserObjectResponseStatus.UserObjectSuccess
        self.ErrorMessage = ''
        self.Properties = None
        if jsonDict: # upon a successful response from the API server jsonDict will be used to populate the DSUserObjectGetAllResponse object with the response data.
            self.UserObjectType = DSUserObjectTypes(jsonDict['UserObjectType'])
            self.UserObjects = jsonDict['UserObjects']
            self.ResponseStatus = DSUserObjectResponseStatus(jsonDict['ResponseStatus'])
            self.UserObjectsCount = jsonDict['UserObjectsCount']
            self.ErrorMessage = jsonDict['ErrorMessage']
            self.Properties = jsonDict['Properties']

class DSUserObjectResponse:
    """
    DSUserObjectResponse is the object returned from the client class' GetItem, CreateItem, UpdateItem and DeleteItem requests.

    Properties
    ----------
    UserObjectId: The ID of the object requested. If the item is deleted, the UserObject property will be None but the UserObjectId field will be populated
    UserObjectType: specifies the returned object type. e.g. DSUserObjectTypes.List, DSUserObjectTypes.TimeSeries, etc.
    UserObject: For all queries bar DeletItem, if the query is successful, this property will contain the user created item requested.
    ResponseStatus: This property will contain a DSUserObjectResponseStatus value. DSUserObjectResponseStatus.UserObjectSuccess represents a successful response.
    ErrorMessage: If ResponseStatus is not DSUserObjectResponseStatus.UserObjectSuccess this status string will provide a description of the error condition.
    Properties: Not currently used and will currently always return None.

    """
    def __init__(self, jsonDict = None):
        self.UserObjectId = None
        self.UserObjectType = DSUserObjectTypes.NoType
        self.UserObject = None
        self.ResponseStatus = DSUserObjectResponseStatus.UserObjectSuccess
        self.ErrorMessage = ''
        self.Properties = None
        if jsonDict: # upon a successful response from the API server jsonDict will be used to populate the DSUserObjectResponse object with the response data.
            self.UserObjectType = DSUserObjectTypes(jsonDict['UserObjectType'])
            self.UserObjectId = jsonDict['UserObjectId']
            self.ResponseStatus = DSUserObjectResponseStatus(jsonDict['ResponseStatus'])
            self.UserObject = jsonDict['UserObject']
            self.ErrorMessage = jsonDict['ErrorMessage']
            self.Properties = jsonDict['Properties']
      