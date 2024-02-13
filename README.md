A python package for retrieving Datastream content, our historical financial database with over 35 million individual instruments
or indicators across all major asset classes, including over 19 million active economic indicators. It features 120 years of data,
across 175 countries � the information you need to interpret market trends, economic cycles, and the impact of world events.

Data spans bond indices, bonds, commodities, convertibles, credit default swaps, derivatives, economics, energy, equities, equity
indices, ESG, estimates, exchange rates, fixed income, funds, fundamentals, interest rates, and investment trusts. Unique content
includes I/B/E/S Estimates, Worldscope Fundamentals, point-in-time data, and Reuters Polls.

Alongside the content, sit a set of powerful analytical tools for exploring relationships between different asset types, with a library of customizable analytical functions.

In-house timeseries can also be uploaded using the package to comingle with Datastream maintained datasets, use with these analytical
tools and displayed in Datastream�s flexible charting facilities in Microsoft Office.�


# Available in this version 

This release supports the following features:

* A DataClient for retrieval of static and timeseries data.
* A TimeserieClient for creating and managing your custom timeseries data.
* An EconomicFilters client for creating and managing custom filters for analysing changes to economics data.


# Dependencies

This package has a dependency on the pandas, requests and ssl packages.


# Prerequisites

You need to have an account to access Datastream content and these credentials need to be permissioned to access our API service.

If you do not have an account, please request Datastream product details [here](https://www.lseg.com/en/data-analytics/products/datastream-macroeconomic-analysis/). If you do have an account, please contact your local LSEG representative to discuss access to the web API service.

In addition to the basic access, you need additional permissions to be able to create and manage custsom timeseries and economic filters. Again, contact your customer representative.


# Upgrading from DatastreamPy-1.0.x packages to DatastreamPy-2.0.x

DatastreamPy version 2.0 modifies the method of invoking the Datastream client. Prior to version 2.0 you needed to supply your credentials in the
client constructor. Whilst you can still supply your credentials in the new constructor, version 2.0 supports client credentials being read directly from a
configuration file. Note, the client constructor has also been renamed as part of the additional support for user created timeseries and economic filters.

```
# In earlier versions (1.0.x), you need to supply your credentials.
ds = dsweb.Datastream(username='YourID', password='YourPwd')

# In version 2.0.x, the client has been renamed from Datastream to DataClient and the credentials can be supplied from a configuration file
ds = dsweb.DataClient('Config.ini')
# or supplied directly as before.
ds = dsweb.DataClient(None, 'YourID', 'YourPwd')
# Note credentials supplied in the constructor will override any credentials set in the configuration file if also provided
ds = dsweb.DataClient('Config.ini', 'YourIdOverride', 'YourPwdOverride')
```

The configuration file has the following sections. Note, the path setting should only be modified for private network configurations (Delivery Direct)
under the advice of your Datastream representative. Please see the final section below for information on proxies and certificates.

```
[url]
path=

[credentials]
username=YourID
password=YourPwd

[proxies]
proxies=

[cert]
sslVerify=

[app]
timeout=  
```


# Getting started

First of all we need to download the package:
```
pip install DatastreamPy
```

Then we just import the library and refer to the following sections demonstrating usage:
```
import DatastreamPy as dsweb
```


# Logging on with your credentials

In order to access Datastream content, you need to logon with your Datastream credentials. Each of the modules can load the credentials from a configuration file, or you can
supply the credentials directly in the constructor:

```
# loading credentials from a config file
ds = dsweb.DataClient('Config.ini')

# loading credentials directly into the constructor
ds = dsweb.DataClient(None, 'YourID', 'YourPwd')
```

For the configuration file option you can specify your credentials with the following configuration section:
```
[credentials]
# Replace YourID and YourPwd values with your specific Datastream credentials.
username=YourID
password=YourPwd
```


# Basic timeseries and static data retrieval

To access timeseries or static data you use the DataClient object.

A sample timeseries request takes the form:

```
ds = dsweb.DataClient('Config.ini')

start_date = '2020-01-01'
end_date = '2022-12-31'

# Retrieve and print out some timeseries data for Apple and Microsoft
history = ds.get_data(tickers = '@AAPL,@MSFT', fields = ['P', 'VO', 'RI'],\
                     kind = 1, start = start_date, end = end_date, freq = 'M')
history.index = pd.date_range(start_date, end_date, freq = 'M')
print (history)
```

A sample static data request takes the form:

```
staticdata = ds.get_data(tickers = '@AAPL,@MSFT', fields = ['NAME', 'ISIN', 'BDATE'], kind = 0)
print(staticdata)
```


# Managing custom timeseries items

Datastream permits users to create and manage custom items. One of these types is custom timeseries data. Clients can upload
their own timeseries data to Datastream�s central systems. These can then be used in combination with Datastream maintained
series in charts and data requests with the full power of the Datastream expressions language.

```
import DatastreamPy as dsweb
import pandas as pd
# We import random only to generate some test data 
import random

# create your client
timeseriesClient = dsweb.TimeseriesClient(None, 'YourID', 'YourPwd')

# Note Timeseries IDs must be 8 uppercase alphanumeric characters in length and start with TS. e.g. TSZZZ001
testID = 'TSZZZ001'

# let us create a data series with quarterly values between 2016-01-01 and 2022-04-01
startDate = date(2016, 1, 1)
endDate = date(2022, 4, 1)
freq = dsweb.DSUserObjectFrequency.Quarterly

# First step is to retrieve the list of supported dates for the above period
dateRangeResp = timeseriesClient.GetTimeseriesDateRange(startDate, endDate, freq)
if dateRangeResp:
    if dateRangeResp.ResponseStatus == dsweb.DSUserObjectResponseStatus.UserObjectSuccess and dateRangeResp.Dates != None:
        # You would normally use the returned supported dates to match up with dates in your data source
        # Here we will just create some random data based on the number of returned dates
        random.seed()
        values = [(random.randint(1000, 20000) / 100) for k in range(0, len(dateRangeResp.Dates))]

        # Construct our timeseries object with the ID, start and end dates, frequency and list of datapoints
        testTs = dsweb.DSTimeSeriesRequestObject(testID, startDate, endDate, freq, values)

        # Set any other optional properties directly
        testTs.DisplayName = 'My first test timeseries' # set to the same as the ID in the response by default.
        testTs.DecimalPlaces = 2 # we created our array of values with 2 decimal places. You can specify 0 to 8 decimal places.
        testTs.Units = "Billions"  # Leave units blank or set with any custom text (max 12 chars).
        # when requested by users in data retrieval, you can specify the quarterly dates to be returned as start, middle or end of 
        # the selected period (frequency). Here we want the quarterly data to be mid period (15th of middle month)
        testTs.DateAlignment = dsweb.DSTimeSeriesDateAlignment.MidPeriod 

        # and create the new item with the overWrite option set to perfrom an update if the timeseries already exists.
        tsResponse = timeseriesClient.CreateItem(testTs, overWrite = True)

        # Any request dealing with a single user created item returns a DSUserObjectResponse.
        # This has ResponseStatus property that indicates success or failure
        if tsResponse.ResponseStatus != dsweb.DSUserObjectResponseStatus.UserObjectSuccess:
            print('Request failed for timeseries with error ' + tsResponse.ResponseStatus.name + ': ' + tsResponse.ErrorMessage, end='\n\n')
        elif tsResponse.UserObject != None:  # The timeseries item won't be returned if you set SkipItem true in CreateItem or UpdateItem
            # Here we simply display the timeseries data using a dataframe.
            tsItem = tsResponse.UserObject
            names = ['Id', 'Desc', 'LastModified', 'StartDate', 'EndDate', 'Frequency', 'NoOfValues']
            coldata = [tsItem.Id, tsItem.Description, tsItem.LastModified.strftime("%Y-%m-%d"), 
                        tsItem.DateInfo.StartDate.strftime("%Y-%m-%d"), tsItem.DateInfo.EndDate.strftime("%Y-%m-%d"),
                        tsItem.DateInfo.Frequency.name, tsItem.DateRange.ValuesCount]
            df = pd.DataFrame(coldata, index=names)
```


You can also obtain summary details for all the timeseries you currently have or retrieve the full details for a specified item. The class also
supports methods that allow you to modify or delete a given timeseries:

```
# list all the custom timeseries you already own
itemsResp = timeseriesClient.GetAllItems()
# Returns a DSUserObjectGetAllResponse which has ResponseStatus property that indicates success or failure for the query
if itemsResp:
    if itemsResp.ResponseStatus != dsweb.DSUserObjectResponseStatus.UserObjectSuccess:
        # Your Datastream Id might not be permissioned for managing user created items on this API
        print('GetAllItems failed with error ' + itemsResp.ResponseStatus.name + ': ' + itemsResp.ErrorMessage, end='\n\n')
    elif itemsResp.UserObjectsCount == 0 or itemsResp.UserObjects == None:
        print('GetAllItems returned zero timeseries items.', end='\n\n')
    else:
        """You do have access to some timeseries.
        # Here we just put the timeseries details into a dataframe and list them
        print('{}{}{}'.format('GetAllItems returned ', itemsResp.UserObjectsCount, ' timeseries items.'))
        data  = []
        colnames = ['Id', 'Start', 'End', 'Freq', 'DPs']
        for tsItem in itemsResp.UserObjects:
            if tsItem:
                rowdata = [tsItem.Id, tsItem.DateInfo.StartDate.strftime("%Y-%m-%d"), tsItem.DateInfo.EndDate.strftime("%Y-%m-%d"),
                           tsItem.DateInfo.Frequency.name, tsItem.DateRange.ValuesCount]
                data.append(rowdata)
        df = pd.DataFrame(data, columns=colnames)
        print(df, end='\n\n')


# To retrieve the full details of a specific timeseries use the GetItem method
tsResponse = timeseriesClient.GetItem(testID)
if tsResponse.ResponseStatus != dsweb.DSUserObjectResponseStatus.UserObjectSuccess:
    print('Request failed for timeseries with error ' + tsResponse.ResponseStatus.name + ': ' + tsResponse.ErrorMessage, end='\n\n')
elif tsResponse.UserObject != None:  # The timeseries item won't be returned if you set SkipItem true in CreateItem or UpdateItem
    # Here we simply display the timeseries data using a dataframe.
    tsItem = tsResponse.UserObject
    names = ['Id', 'Desc', 'LastModified', 'StartDate', 'EndDate', 'Frequency', 'NoOfValues']
    coldata = [tsItem.Id, tsItem.Description, tsItem.LastModified.strftime("%Y-%m-%d"), 
                tsItem.DateInfo.StartDate.strftime("%Y-%m-%d"), tsItem.DateInfo.EndDate.strftime("%Y-%m-%d"),
                tsItem.DateInfo.Frequency.name, tsItem.DateRange.ValuesCount]
    df = pd.DataFrame(coldata, index=names)

# updating an item takes the same parameters as CreateItem. See how we construct testTs above
tsResponse = timeseriesClient.UpdateItem(testTs)

# And we can delete the item using the test ID we defined in the CreateItem step
delResp = timeseriesClient.DeleteItem(testID)
```



# Creating custom economic filters

Datastream provides access to over 19 million economic series. With coverage of this extent, it can be difficult to prioritise which
region, country, sector or industry to analyse and investigate. With this in mind, clients that access the full Datatstream Web Service
can now poll for the latest changes and corrections to any of the economic series.

Even with polling for changes and corrections, the large number of economic series supported can produce a large number of updates to
process each day. To reduce the number of updates, Datastream provides a global filter, DATASTREAM_KEYIND_GLOBAL, that comprises the 
25K most prominent series. Querying for updates using this filter can significantly reduce the number of updates reported.

Clients can now also create their own custom filters comprising up to 100K series and use these to query for changes and corrections.
This section demonstrates using the DatastreamPy package to create and manage custom filters.

First of all let us see how to query for a list of economic items that have updated since a given start date. Then we will demonstrate
using the built in filter DATASTREAM_KEYIND_GLOBAL to restrict the number of updates to a specified list of economic series.

Polling for recent changes requires using a starting sequence ID and retrieving up to 10K updates with each polling request. As we process
each response, we retrieve in the response the next sequence ID to request in the chain of updates until we are instructed that there
are no more updates. We would then use the returned final sequence ID to poll every 10 minutes until we receive a response with new updates.

```
import DatastreamPy as dsweb
import pandas as pd

# Try creating the client by replacing 'YourID' and 'YourPwd' with your own credentials.
econFilterClient = EconomicFilters(None, 'YourID', 'YourPwd')

# We'll start searching for any changes beginning 3 weeks ago (you can go back a maximum of 28 days)
# NB: Setting the timestamp to None will set the start date at 00:00 hours on the previous weekday from now
updatesResp = econFilterClient.GetEconomicChanges(datetime.today() - timedelta(days=21))
# this should tell us the start sequence ID for updates from the given start datetime and how many updates we have pending
sequenceId = 0 # placeholder which we will update and use later in the demo
if updatesResp:
    if updatesResp.ResponseStatus != dsweb.DSFilterResponseStatus.FilterSuccess:
        # Any filter request errors, such as invalid filter, not being explicity permissioned to use custom economic filters, etc.
        print('GetEconomicChanges failed with error ' + updatesResp.ResponseStatus.name + ': ' + updatesResp.ErrorMessage)
    else:
        sequenceId = updatesResp.NextSequenceId
        # we'll now use this starting sequence in the following test cells.
        updatesResp = econFilterClient.GetEconomicChanges(None, sequenceId)
        if updatesResp:
            if updatesResp.ResponseStatus != dsweb.DSFilterResponseStatus.FilterSuccess:
                print('GetEconomicChanges failed with error ' + updatesResp.ResponseStatus.name + ': ' + updatesResp.ErrorMessage)
            else:
                if updatesResp.Updates and updatesResp.UpdatesCount > 0:
                    # You have some updates; process them and retrieve the NextSequenceID for retrieving any subsequent updates.
                    print ('You have {:,} new updates:'.format(updatesResp.UpdatesCount))
                    updates = [[update.Series, update.Frequency.name, update.Updated.strftime('%Y-%m-%d %H:%M:%S')] for update in updatesResp.Updates]
                    df = pd.DataFrame(data=updates, columns=['Series', 'Frequency', 'Updated'])
                    print(df, end='\n\n')
                if updatesResp.UpdatesPending:
                    print ('You still have {:,} updates pending starting from new sequence {}.'.format(updatesResp.PendingCount, updatesResp.NextSequenceId))
                else:
                    print ('You have no more updates pending. Use the new sequence {} to begin polling for future updates.'.format(updatesResp.NextSequenceId))

# Whilst updatesResp.UpdatesPending is True you would retrieve the next request sequence from updatesResp.NextSequenceId and request the next block of updates.
# You would continue walking through the chain until you receive the final chain with updatesResp.UpdatesPending returning False
# At this point the value in updatesResp.NextSequenceId will contain the ID for the next update that occurs. You then use this ID in periodic polls
# (minimum every 10 minutes) to wait for new updates to be notified.
```

In the previous example, where we started searching for updates starting from 3 weeks ago, you will find you have a large chain of updates to query
for in order to process the updates across the entire universe of +19 million series. 

To optimise this search, Datastream provides a custom filter, DATASTREAM_KEYIND_GLOBAL, which defines the most popular economic series. Using this filter drastically cuts
down the number of updates you will receive and process.

```
print('Let us repeat the processing using the same starting sequence ID. This time we will use the global filter DATASTREAM_KEYIND_GLOBAL.')
updatesResp = econFilterClient.GetEconomicChanges(None, sequenceId, 'DATASTREAM_KEYIND_GLOBAL')
if updatesResp:
    if updatesResp.ResponseStatus != dsweb.DSFilterResponseStatus.FilterSuccess:
        print('GetEconomicChanges failed with error ' + updatesResp.ResponseStatus.name + ': ' + updatesResp.ErrorMessage)
    else:
        if updatesResp.Updates and updatesResp.UpdatesCount > 0:
            # You have some updates; process them.
            print ('You have {:,} new updates:'.format(updatesResp.UpdatesCount))
            updates = [[update.Series, update.Frequency.name, update.Updated.strftime('%Y-%m-%d %H:%M:%S')] for update in updatesResp.Updates]
            df = pd.DataFrame(data=updates, columns=['Series', 'Frequency', 'Updated'])
            print(df, end='\n\n')
        if updatesResp.UpdatesPending:
            print ('You still have {:,} updates pending starting from new sequence {}.'.format(updatesResp.PendingCount, updatesResp.NextSequenceId))
        else:
            print ('You have no more updates pending. Use the new sequence {} and the filter to begin polling for future updates.'.format(updatesResp.NextSequenceId))
```

You can also create your own custom economic filter to specifically query for changes just against this restricted set of economic series.
```
# This example demonstrates how to create a new filter.  We will define this filter with ID MyTempTestFilter
demoId = 'MyTempTestFilter'

# Let us create the filter. We'll create it with 10 valid constituents but also include two invalid items, BADINST1 and 
# BADINSTFMT, to also demonstrate how the server will reject invalid or unsupported items. BADINST1 has valid syntax but is not a 
# valid series. BADINSTFMT is not the correct economic series format (7 to 9 chars only).

#  NB: These are 10 random series chosen because they update frequently.
initialConstituents = ['UKEPUPO', 'USEPUPO', 'USEPUEQ', 'IDTVALS', 'IDTVOLS', 'IDTVADS', 
                       'IDTVALP', 'IDTVOLP', 'IDTVADP', 'IDTVAFP', 'BADINST1', 'BADINSTFMT']

# we construct DSEconomicsFilter with our test ID MyTempTestFilter, assign the constituents, set the description and call CreateFilter
myFilter = DSEconomicsFilter()
myFilter.FilterId = demoId

myFilter.Constituents = initialConstituents
myFilter.Description = 'MyTempTestFilter for testing.'
    
print('Creating private filter ' + demoId + '...')
filterResp = econFilterClient.CreateFilter(myFilter)
if filterResp:
    # Any request dealing with a single filter returns a DSEconomicsFilterResponse.
    # This has ResponseStatus property that indicates success or failure
    if filterResp.ResponseStatus != dsweb.DSFilterResponseStatus.FilterSuccess:
        print('Request failed for filter ' + filterName + ' with error ' + filterResp.ResponseStatus.name + ': ' + filterResp.ErrorMessage, end='\n\n')
    elif filterResp.Filter != None: #display it
        filter = filterResp.Filter
        names = ['FilterId', 'OwnerId', 'Shared?', 'LastModified', 'Description', 'No. of Constituents']
        data = [filter.FilterId, filter.OwnerId, 'Yes' if bool(filter.Shared) else 'No', 
                filter.LastModified.strftime('%Y-%m-%d %H:%M:%S'), filter.Description, filter.ConstituentsCount]
        df = pd.DataFrame(data, index=names)
        print(df)

        print('The filter contains the following constituents:')
        df = pd.DataFrame(filter.Constituents)
        print(df, end='\n\n')
            
        if filterResp.ItemErrors and len(filterResp.ItemErrors) > 0:
            print('The service did not add the following items as they are invalid or are not supported:')
            df = pd.DataFrame(filterResp.ItemErrors)
            print(df, end='\n\n')
```

You can then use your new filter in any queries for new updates:
```
updatesResp = econFilterClient.GetEconomicChanges(None, sequenceId, 'MyTempTestFilter')
# see the initial GetEconomicChanges example for how to process the result
```

The EconomicsFilter object also supports the following methods:
* GetAllFilters - returns a summary list of all filters owned by you and/or the globally available filters.
* GetFilter - returns the full details, including constituents, for a specified filter.
* UpdateFilter - allows you to append or remove specified constituent series or replace all the constituents.
* DeleteFilter - allows you to delete one of your filters.



# Error handling

The DatastreamPy package makes web API queries against our servers. This can result in network errors or authentication errors if you
are not authorised to use the service.

DSUserObjectFault exceptions are thrown for any non-network errors from the API. These are returned for the following reasons:
* Invalid credentials
* Empty credentials
* Access blocked due to missuse of the service (for filter requests after credentials validation).

Any general network errors such as firewall issues, proxy issues, general connectivity issues, etc. will throw an Exception object.
This will typically be returned from the sessions.py module directly, or an exception will be thrown using the Response.raise_for_status()
should the returned response not be processed correctly.


```
try:
    # Try creating the client by replacing 'YourID' and 'YourPwd' with your own credentials.
    econFilterClient = dsweb.EconomicFilters(None. 'YourId', 'YourPwd')
    print('Successfully created the EconomicFilters client.', end='\n\n')

except DSUserObjectFault as dsFault:
    print('EconomicFilters() failed returning a DSUserObjectFault exception:')
    print(dsFault)
except Exception as exp:
    print('A network error occurred.')
    print(exp)
```


For requests against the TimeseriesClient or EconomicFilters client objects (e.g. GetAllItems), the response object contains either a
DSUserObjectResponseStatus or DSFilterResponseStatus object in the ResponseStatus property. If the status is not UserObjectSuccess or 
FilterSuccess, then this specifies a logical failure with the request on our server. In this event, the ErrorMessage property will
specify the reason for the error.

For DSUserObjectResponseStatus the following status values exist:
* UserObjectSuccess: The request succeeded and the response object's UserObject(s) property should contain the (updated) object (except for DeleteItem method).
* UserObjectPermissions: Users need to be specifically permissioned to create custom objects. This flag is set if you are not currently permissioned.
* UserObjectNotPresent: Returned if the requested ID does not exist.
* UserObjectFormatError: Returned if your request object is not in the correct format.
* UserObjectTypeError: Returned if your supplied object is not the same as the type specified.
* UserObjectError:  The generic error flag. This will be set for any error not specified above. (e.g. object not present, etc.)

For DSFilterResponseStatus the following status values exist:
* FilterSuccess: The request succeeded and the response object's Filter property should contain the (updated) filter (except for DeleteFilter method).
* FilterPermissions: Users need to be specifically permissioned to create custom filters. This flag is set if you are not currently permissioned.
* FilterNotPresent: Returned if the requested ID does not exist.
* FilterFormatError: Returned if your request filter ID is not in the correct format, or if you try and modify a Datastream global filter (ID begins DATASTREAM*).
* FilterSizeError: Returned if your call to CreateFilter or ModifyFilter contains a list with zero or in excess of the 100K constituents.
* FilterConstituentsError: Returned if your supplied filter constituent list (on CreateFilter) contains no valid economic series. The filter won't be created.
* FilterError:  The generic error flag. This will be set for any error not specified above. (e.g. Requested filter ID is not present)


# Proxies and certificates

The module uses the requests package to make the web api queries using a sessions object internally:
```
httpResponse = self._reqSession.post(reqUrl, json = jsonRequest,  proxies = self._proxies, verify = self._certfiles, cert = self._sslCert, timeout = self._timeout)
```

You can provide the proxy and certificate details in the constructor of each client object:

```
x = EconomicFilters(None, username = 'YourID', password = 'YourPwd', proxies = x, sslVerify = y, sslCert = z)
```

Alternatively, the proxy and the root certificates for verification can be set in the configuration file:

```
[proxies]
# of the form: { 'http' : proxyHttpAddress,  'https' : proxyHttpsAddress }
proxies=

[cert]
# option to supply a specific python requests verify option. 
sslVerify=

[app]
timeout=  
```

For specifying the proxy see the verify and cert properties on the session post method, see [this link](https://docs.python-requests.org/en/latest/user/advanced/).

If you do not override a root certificates option for the verify parameter, the code will use the ssl package on Windows platforms
to import the root certificates into a temporary file tempCertFile.pem and uses that as the verify parameter. For non-Windows
platforms, the package will use the results of the requests.certs.where() method to set the verify parameter. 

