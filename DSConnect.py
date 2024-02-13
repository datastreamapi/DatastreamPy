"""
Datastream User Created Items
-----------------------------

Datastream permits users to create and manage custom items. Five user created item types are supported:
    Timeseries - Upload your own timeseries data to Datastream's central systems. These can then be used in combination with Datastream maintained series in charts 
                 and data requests with the full power of the Datastream expressions language.
    Lists - Create and manage your own lists of constituent items. These can then be used to request static data for the constituents of the lists.
    Expressions - Combine Datastream data items with analytical functions to create custom expressions.
    Indices - Create an index based on a portfolio of instruments and measure the performance against variations on established market indices and other benchmarks.
    Regression - Create a regression model to analyse the extent to which one time series (the dependent variable) is influenced by other time series.

Each of these five user created types have their own client connections which manage the interaction with the API service. Each client customises the requests
and responses specific to each user type. For example, the Timeseries client class supports a method for retrieving the dates supported by Datastream between two 
given dates at a specified frequency. Similarily, the Expression client class supports a method for verifying a proposed expression is valid.

This file defines a DSConnect class which is the base class for the specific client classes supporting each user created type. It basically handles the raw JSON
query and response protocol for the API service.

Most of the methods here are private and designed to be called from the derived user created type classes.
"""

import requests
import json
import pytz
from datetime import datetime, timedelta, date
from enum import Enum
import re
import platform
import configparser
import time
import ast
from .DSUserDataObjectBase import DSUserObjectFault, DSUserObjectLogLevel, DSUserObjectLogFuncs, DSUserObjectDateFuncs, DSPackageInfo

class DSUserCreatedJsonDateTimeEncoder(json.JSONEncoder):
    """ 
    DSUserCreatedJsonDateTimeEncoder is used in the conversion of datetime objects into JSON format. It's passed into json.dumps()

    Example:
    jsonText = json.dumps(reqObject, cls = DSUserCreatedJsonDateTimeEncoder)
    """
    def default(self, obj):
        if isinstance(obj, datetime) or isinstance(obj, date):
            return DSUserObjectDateFuncs.toJSONdate(obj)
        # else fall through to json default encoder
        return json.JSONEncoder.default(self, obj)


class DSConnect:
    """DSConnect connects to the Datastream web service on behalf of derived classes and helps to send and retrieve data"""
    
#--------------- Constructor ----------------------------------------------------------------------------------------  
    def __init__(self, config = None, username = None, password = None, proxies = None, sslVerify = None, sslCert = None):
        """
        Constructor: user details can be supplied from a config file or passed directly as parameters in the constructor of the derived user object type class.

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


        # first logon with your credentials. Creating a TimeseriesClient instance (derived from DSConnect) with your credentials automatically logs on for you 
        timeseriesClient = TimeseriesClient('config.ini')

        2) Bypassing a config file and using your credentials directly:

        timeseriesClient = TimeseriesClient(None, 'YourId', 'YourPwd')
        """

        # Properties
        self.url = "https://product.datastream.com" # Warning: Only override the url for the API service if directed to by LSEG.
        self.username = None
        self.password = None
        self.token = None # when you logon your token for subsequent queries is stored here
        self.tokenExpiry = None # tokens are typically valid for 24 hours. The client will automatically renew the token if you make request within 15 minutes of expiry
        self.navigatorSeriesUrl = None # The url to browse and search Datastream Navigator for specific instruments
        self.navigatorDatatypesUrl = None # The url to browse and search Datastream Navigator for specific datatypes
        self._proxies = None
        self._sslCert = None
        self._certfiles = None
        # some restricted settings that allow us to track version usage on our servers.
        self._reqSession = requests.Session()
        self._timeout = 300
        self._reqSession.headers['User-Agent'] = self._reqSession.headers['User-Agent'] + DSPackageInfo.UserAgent
        self._apiSchemaNamespace = ':http://dsws.datastream.com/client/V1/'

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


        # set the full reference to the API service from the supplied url
        self.url = self.url +'/DSWSClient/V1/DSUserDataService.svc/rest/'

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
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'DSConnect._get_Token', 'Requesting new token')
            token_url = self.url + 'GetToken'
            tokenReq = { "Password" : self.password,
                         "Properties" : [{ "Key" : "__AppId", "Value" : DSPackageInfo.appId}, { "Key" : "ReturnOptions", "Value" : "NavigatorSeries,NavigatorDatatypes"}],
                         "UserName" : self.username}
            #Post Token Request
            json_Response = self._get_json_Response(token_url, tokenReq)
            self.tokenExpiry = DSUserObjectDateFuncs.jsonDateTime_to_datetime(json_Response['TokenExpiry'])
            self.token = json_Response['TokenValue']

            # Check the Properties collection for the urls that specify where to browse for Datastream Navigator
            if json_Response['Properties'] and len(json_Response['Properties']) > 0:
                for prop in json_Response['Properties']:
                    if isinstance(prop['Key'], str) and prop['Key'].lower() == 'navigatorseries':
                        self.navigatorSeriesUrl = prop['Value'] # The url to browse and search Datastream Navigator for specific instruments
                    elif isinstance(prop['Key'], str) and prop['Key'].lower() == 'navigatordatatypes':
                        self.navigatorDatatypesUrl = prop['Value'] # The url to browse and search Datastream Navigator for specific datatypes

            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogInfo, 'DatastreamPy', 'DSConnect._get_Token', 'New token received.')
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSConnect._get_Token', 'Exception occured.', exp)
            raise exp

    def IsValid(self):
        return isinstance(self.token, str) and len(self.token) > 0 and isinstance(self.tokenExpiry, datetime)

    def Check_Token(self):
        if not self.IsValid():
            raise Exception("You are not logged on. Please recreate this client object supplying valid user credentials.")
        # A function called before every query to check and renew the token if within 15 minutes of expiry time or later
        timeRenew = datetime.utcnow() + timedelta(minutes = 15) # curiously utcnow() method doesn't set the time zone to utc. We need to do so to compare with token.
        timeRenew = datetime(timeRenew.year, timeRenew.month, timeRenew.day, timeRenew.hour, timeRenew.minute, timeRenew.second, 0, tzinfo=pytz.utc)
        if self.tokenExpiry <= timeRenew : 
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogTrace, 'DatastreamPy', 'DSConnect.Check_Token', 'Token has expired. Refrefreshing')
            self._get_Token()


    def _json_Request(self, reqObject, customEncoderClass = DSUserCreatedJsonDateTimeEncoder):
        # An internal method to convert the request object into JSON for sending to the API service
        try:
            #convert the dictionary (raw text) to json text first, encoding any datetimes as json /Date() objects
            jsonText = json.dumps(reqObject, cls = customEncoderClass)
            byteTemp = bytes(jsonText,'utf-8')
            byteTemp = jsonText.encode('utf-8')
            #convert the json Text to json formatted Request
            jsonRequest = json.loads(byteTemp)
            return jsonRequest
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSConnect._json_Request', 'Exception occured:', exp)
            raise exp


    def _get_json_Response(self, reqUrl, raw_request, customEncoderClass = DSUserCreatedJsonDateTimeEncoder):
        # This method makes the query against the API service and does some basic error handling
        try:
            #convert raw request to json format before post
            jsonRequest = self._json_Request(raw_request, customEncoderClass)

            # post the request
            DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogTrace, 'DatastreamPy', 'DSConnect._get_json_Response', 'Starting web request:', raw_request)
            httpResponse = self._reqSession.post(reqUrl, json = jsonRequest,  proxies = self._proxies, verify = self._certfiles, cert = self._sslCert, timeout = self._timeout)

            # check the response
            if httpResponse.ok:
                json_Response = dict(httpResponse.json())
                DSUserObjectLogFuncs.LogDetail(DSUserObjectLogLevel.LogTrace, 'DatastreamPy', 'DSConnect._get_json_Response', 'Web response received:', json_Response)
                return json_Response
            elif httpResponse.status_code == 400 or httpResponse.status_code == 403:
                # possible DSFault exception returned due to permissions, etc
                try:
                    tryJson = json.loads(httpResponse.text)
                    if 'Message' in tryJson.keys() and 'Code' in tryJson.keys():
                        faultDict = dict(tryJson)
                        DSUserObjectLogFuncs.LogError('DatastreamPy', 'DSConnect._get_json_Response', 'API service returned a DSFault:', 
                                                            faultDict['Code'] + ' - ' + faultDict['Message'])
                        raise DSUserObjectFault(faultDict)
                except json.JSONDecodeError as jdecodeerr:
                    pass
            # unexpected response so raise as an error
            httpResponse.raise_for_status()
        except json.JSONDecodeError as jdecodeerr:
            DSUserObjectLogFuncs.LogError('DatastreamPy', 'DSConnect._get_json_Response', 'JSON decoder Exception occured:', jdecodeerr.msg)
            raise
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSConnect._get_json_Response', 'Exception occured:', exp)
            raise
 

     
    
    
