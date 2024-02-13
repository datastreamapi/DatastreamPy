
from itertools import count
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
import platform
import configparser
import re
from enum import IntEnum
import ast

from .DS_Requests import TokenRequest, Instrument, Properties, DataRequest, DataType, Date
from .DSUserDataObjectBase import DSUserObjectFault, DSPackageInfo, DSUserObjectLogLevel, DSUserObjectLogFuncs

class DSSymbolResponseValueType(IntEnum):
    Error = 0
    Empty = 1
    Bool = 2
    Int = 3
    DateTime = 4
    Double = 5
    String = 6
    BoolArray = 7
    IntArray = 8
    DateTimeArray = 9
    DoubleArray = 10
    StringArray = 11
    ObjectArray = 12
    NullableBoolArray = 13
    NullableIntArray = 14
    NullableDateTimeArray = 15
    NullableDoubleArray = 16

#--------------------------------------------------------------------------------------
class DataClient:
    """DataClient helps to retrieve data from DSWS web rest service"""
    
#--------Constructor ---------------------------  
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


        # first logon with your credentials. Creating a DataClient instance (derived from DSConnect) with your credentials automatically logs on for you 
        dataClient = DataClient('config.ini')

        2) Bypassing a config file and using your credentials directly:

        dataClient = DataClient(None, 'YourId', 'YourPwd')
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
        self._timeout = 300
        self._reqSession = requests.Session()
        self._reqSession.headers['User-Agent'] = self._reqSession.headers['User-Agent'] + DSPackageInfo.UserAgent

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
        self.url = self.url +'/DSWSClient/V1/DSService.svc/rest/'

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
                # Using SSL instead of WinCertStore package as it is depricated.
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
            self._get_token()
        else:
            raise Exception("You must supply some user credentials.")

#-------------------------------------------------------  
#------------------------------------------------------- 
    def post_user_request(self, tickers, fields=None, start='', end='', freq='', kind=1, retName=False):
        """ This function helps to form requests for get_bundle_data. 
            Each request is converted to JSON format.
            
            Args:
               tickers: string, Dataypes 
               fields: List, default None
               start: string, default ''
               end : string, default ''
               freq : string, default '', By deafult DSWS treats as Daily freq
               kind: int, default 1, indicates Timeseries as output

          Returns:
                  Dictionary"""

            
        if fields == None:
            fields=[]
        
        index = tickers.rfind('|')
        propList = []
        try:
            if index == -1:
                instrument = Instrument(tickers, None)
            else:
                #Get all the properties of the instrument
                instprops = []
                if tickers[index+1:].rfind(',') != -1:
                    propList = tickers[index+1:].split(',')
                    for eachProp in propList:
                        instprops.append(Properties(eachProp, True))
                else:
                    propList.append(tickers[index+1:])
                    instprops.append(Properties(tickers[index+1:], True))

                instrument = Instrument(tickers[0:index], instprops)
                        
            datypes=[]
            if 'N' in propList:
                prop = [{'Key':'ReturnName', 'Value':True}] 
                retName = True
            else:
                prop = None

            
            if len(fields) > 0:
                for eachDtype in fields:
                    datypes.append(DataType(eachDtype, prop))
            else:
                datypes.append(DataType(fields, prop))
                        
            date = Date(start, freq, end, kind)
            request = {"Instrument":instrument,"DataTypes":datypes,"Date":date}
            return request, retName
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.post_user_request', 'Exception occured:', exp)
            raise exp
            
    def get_data(self, tickers, fields=None, start='', end='', freq='', kind=1):
        """This Function processes a single JSON format request to provide
           data response from DSWS web in the form of python Dataframe
           
           Args:
               tickers: string, Dataypes 
               fields: List, default None
               start: string, default ''
               end : string, default ''
               freq : string, default '', By deafult DSWS treats as Daily freq
               kind: int, default 1, indicates Timeseries as output
               retName: bool, default False, to be set to True if the Instrument
                           names and Datatype names are to be returned

          Returns:
                  DataFrame."""
                 

        getData_url = self.url + "GetData"
        raw_dataRequest = ""
        json_dataRequest = ""
        json_Response = ""
        
        if fields == None:
            fields = []
        
        try:
            retName = False
            req, retName = self.post_user_request(tickers, fields, start, end, freq, kind, retName)
            datarequest = DataRequest()
            self.Check_Token() # check and renew token if within 15 minutes of expiry
            raw_dataRequest = datarequest.get_Request(req, self.token)
               
            if (raw_dataRequest != ""):
                json_Response = self._get_json_Response(getData_url, raw_dataRequest)
                
                #format the JSON response into readable table
                if 'DataResponse' in json_Response:
                    if retName:
                        self._get_metadata(json_Response['DataResponse'])
                    response_dataframe = self._format_Response(json_Response['DataResponse'])
                    return response_dataframe
                else:
                    if 'Message' in json_Response:
                        raise Exception(json_Response['Message'])
                    return None
            else:
                return None
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.get_data', 'Exception occured:', exp)
            raise exp
    
    def get_bundle_data(self, bundleRequest=None, retName=False):
        """This Function processes a multiple JSON format data requests to provide
           data response from DSWS web in the form of python Dataframe.
           Use post_user_request to form each JSON data request and append to a List
           to pass the bundleRequset.
           
            Args:
               bundleRequest: List, expects list of Datarequests 
               retName: bool, default False, to be set to True if the Instrument
                           names and Datatype names are to be returned

            Returns:
                  DataFrame."""

        getDataBundle_url = self.url + "GetDataBundle"
        raw_dataRequest = ""
        json_dataRequest = ""
        json_Response = ""
       
        if bundleRequest == None:
            bundleRequest = []
        
        try:
            datarequest = DataRequest()
            self.Check_Token() # check and renew token if within 15 minutes of expiry
            raw_dataRequest = datarequest.get_bundle_Request(bundleRequest, self.token)

            if (raw_dataRequest != ""):
                 json_Response = self._get_json_Response(getDataBundle_url, raw_dataRequest)
                 if 'DataResponses' in json_Response:
                     if retName:
                         self._get_metadata_bundle(json_Response['DataResponses'])
                     response_dataframe = self._format_bundle_response(json_Response)
                     return response_dataframe
                 else:
                    if 'Message' in json_Response:
                        raise Exception(json_Response['Message'])
                    return None
            else:
                return None
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.get_bundle_data', 'Exception occured:', exp)
            raise exp
    
#------------------------------------------------------- 
#-------------------------------------------------------             
#-------Helper Functions---------------------------------------------------
    def _get_Response(self, reqUrl, raw_request):
        try:
            #convert raw request to json format before post
            jsonRequest = self._json_Request(raw_request)
            
            http_Response = self._reqSession.post(reqUrl, json=jsonRequest,  proxies=self._proxies, verify = self._certfiles, cert = self._sslCert, timeout= self._timeout)
            return http_Response
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.get_Response', 'Exception occured:', exp)
            raise exp

        
    def _get_json_Response(self, reqUrl, raw_request):
        try:
          httpResponse = self._get_Response(reqUrl, raw_request)
          # check the response
          if httpResponse.ok:
              json_Response = dict(httpResponse.json())
              return json_Response
          elif httpResponse.status_code == 400 or httpResponse.status_code == 403:
                # possible DSFault exception returned due to permissions, etc
                try:
                    tryJson = json.loads(httpResponse.text)
                    if 'Message' in tryJson.keys() and 'Code' in tryJson.keys():
                        faultDict = dict(tryJson)
                        raise DSUserObjectFault(faultDict)
                except json.JSONDecodeError as jdecodeerr:
                    pass
          # unexpected response so raise as an error
          httpResponse.raise_for_status()
        except json.JSONDecodeError as jdecodeerr:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.get_json_Response', 'JSON decoder Exception occured:', jdecodeerr)
            raise
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.get_json_Response', 'Exception occured:', exp)
            raise
    
    def _get_token(self, isProxy=False):
        token_url = self.url + "GetToken"
        try:
            propties = []
            propties.append(Properties("__AppId", DSPackageInfo.appId))
            propties.append(Properties("ReturnOptions", "NavigatorSeries,NavigatorDatatypes"))

            tokenReq = TokenRequest(self.username, self.password, propties)
            raw_tokenReq = tokenReq.get_TokenRequest()
            
            #Post the token request to get response in json format
            json_Response = self._get_json_Response(token_url, raw_tokenReq)
            self.tokenExpiry = self.jsonDateTime_to_datetime(json_Response['TokenExpiry'])
            self.token = json_Response['TokenValue']

            # Check the Properties collection for the urls that specify where to browse for Datastream Navigator
            if json_Response['Properties'] and len(json_Response['Properties']) > 0:
                for prop in json_Response['Properties']:
                    if isinstance(prop['Key'], str) and prop['Key'].lower() == 'navigatorseries':
                        self.navigatorSeriesUrl = prop['Value'] # The url to browse and search Datastream Navigator for specific instruments
                    elif isinstance(prop['Key'], str) and prop['Key'].lower() == 'navigatordatatypes':
                        self.navigatorDatatypesUrl = prop['Value'] # The url to browse and search Datastream Navigator for specific datatypes
            
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.get_token', 'Exception occured:', exp)
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
            self._get_token()

    
    def _json_Request(self, raw_text):
        #convert the dictionary (raw text) to json text first
        jsonText = json.dumps(raw_text)
        byteTemp = bytes(jsonText,'utf-8')
        byteTemp = jsonText.encode('utf-8')
        #convert the json Text to json formatted Request
        jsonRequest = json.loads(byteTemp)
        return jsonRequest

    def jsonDateTime_to_datetime(self, jsonDate):
        # Convert a JSON /Date() string to a datetime object
        if jsonDate is None:
            return None
        try:
            match = re.match(r"^(/Date\()(-?\d*)(\)/)", jsonDate)
            if match == None:
                match = re.match(r"^(/Date\()(-?\d*)([+-])(..)(..)(\)/)", jsonDate)
            if match:
                return datetime(1970, 1, 1, tzinfo=pytz.utc) + timedelta(seconds=float(match.group(2))/1000)
            else:
                raise Exception("Invalid JSON Date: " + jsonDate)
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.jsonDateTime_to_datetime', 'Exception occured:', exp)
            raise exp
            
    def _get_Date(self, jsonDate):
        try:
            #match = re.match("^/Date[(][0-9]{13}[+][0-9]{4}[)]/", jsonDate)
            match = re.match(r"^(/Date\()(-?\d*)([+-])(..)(..)(\)/)", jsonDate)
            if match:
                #d = re.search('[0-9]{13}', jsonDate)
                d = float(match.group(2))
                ndate = datetime(1970,1,1) + timedelta(seconds=float(d)/1000)
                utcdate = pytz.UTC.fromutc(ndate).strftime('%Y-%m-%d')
                return utcdate
            else:
                raise Exception("Invalid JSON Date")
        except Exception as exp:
            DSUserObjectLogFuncs.LogException('DatastreamPy', 'DSResponse.get_Date', 'Exception occured:', exp)
            raise exp
            
            
    
    def _get_DatatypeValues(self, jsonResp):
        multiIndex = False
        df = pd.DataFrame()
        valDict = {"Instrument":[],"Datatype":[],"Value":[],"Currency":[]}

        for item in jsonResp['DataTypeValues']: 
            datatype = item['DataType']
            
            for i in item['SymbolValues']:
               instrument = i['Symbol']
               currency = None
               if 'Currency' in i:
                   currency = i['Currency'] if i['Currency'] else 'NA'

               valDict["Datatype"].append(datatype)
               valDict["Instrument"].append(instrument)
               if currency:
                   valDict['Currency'].append(currency)
                   colNames = (instrument, datatype, currency)
               else:
                   colNames = (instrument, datatype)

               values = i['Value']
               valType = i['Type']

               #Handling all possible types of data as per DSSymbolResponseValueType

               #These value types return an array
               if valType in [DSSymbolResponseValueType.BoolArray, 
                              DSSymbolResponseValueType.IntArray, 
                              DSSymbolResponseValueType.DateTimeArray,
                              DSSymbolResponseValueType.DoubleArray,
                              DSSymbolResponseValueType.StringArray,
                              DSSymbolResponseValueType.ObjectArray,
                              DSSymbolResponseValueType.NullableBoolArray,
                              DSSymbolResponseValueType.NullableIntArray,
                              DSSymbolResponseValueType.NullableDateTimeArray,
                              DSSymbolResponseValueType.NullableDoubleArray]:
                    #The array can be of bool, double, int, string, dates or Object

                    #Check if the array of has JSON date string and convert each to Datetime
                    temp = [self._get_Date(x) if isinstance(x, str) and x.startswith('/Date(') else x for x in values]
                    df[colNames] = temp
                    
                    if len(values) > 1:
                        multiIndex = True
                    else:
                        multiIndex = False
                        valDict["Value"].append(values[0])  

               #These value types return single value
               elif valType in [DSSymbolResponseValueType.Empty,
                                DSSymbolResponseValueType.Bool,
                                DSSymbolResponseValueType.Int,
                                DSSymbolResponseValueType.DateTime,
                                DSSymbolResponseValueType.Double,
                                DSSymbolResponseValueType.String]:
                   temp = self._get_Date(values) if isinstance(values, str) and values.startswith('/Date(') else values 
                   valDict["Value"].append(temp)
               elif valType == DSSymbolResponseValueType.Error:
                     #Error Returned
                     valDict["Value"].append(values)

        if multiIndex:
            if currency:
                 df.columns = pd.MultiIndex.from_tuples(df.columns, names=['Instrument','Field','Currency'])
            else:
                 df.columns = pd.MultiIndex.from_tuples(df.columns, names=['Instrument','Field'])
                   
        if not multiIndex:
            indexLen = range(len(valDict['Instrument']))
            if valDict['Currency']:
                newdf = pd.DataFrame(data=valDict,columns=["Instrument", "Datatype", "Value", "Currency"],
                                 index=indexLen)
            else:
                newdf = pd.DataFrame(data=valDict,columns=["Instrument", "Datatype", "Value"],
                                 index=indexLen)
            return newdf
        return df 
            
    def _format_Response(self, response_json):
        # If dates is not available, the request is not constructed correctly
        response_json = dict(response_json)
        if 'Dates' in response_json:
            dates_converted = []
            if response_json['Dates'] != None:
                dates = response_json['Dates']
                for d in dates:
                    dates_converted.append(self._get_Date(d))
        else:
            return 'Error - please check instruments and parameters (time series or static)'
        
        # Loop through the values in the response
        dataframe = self._get_DatatypeValues(response_json)
        if (len(dates_converted) == len(dataframe.index)):
            if (len(dates_converted) > 1):
                #dataframe.insert(loc = 0, column = 'Dates', value = dates_converted)
                dataframe.index = dates_converted
                dataframe.index.name = 'Dates'
        elif (len(dates_converted) == 1):
            dataframe['Dates'] = dates_converted[0]
        
        return dataframe

    def _format_bundle_response(self,response_json):
        formattedResp = []
        for eachDataResponse in response_json['DataResponses']:
            df = self._format_Response(eachDataResponse)
            formattedResp.append(df)      
           
        return formattedResp
   
       
    def _get_metadata(self, jsonResp):
        names = {}
        if jsonResp['SymbolNames']:
            for i in jsonResp['SymbolNames']:
                names.update({i['Key']: i['Value']})
                
        if jsonResp['DataTypeNames']:
            for i in jsonResp['DataTypeNames']:
                names.update({i['Key']: i['Value']})
        

    def _get_metadata_bundle(self, jsonResp):
        for eachDataResponse in jsonResp:
            self._get_metadata(eachDataResponse)
#-------------------------------------------------------------------------------------

