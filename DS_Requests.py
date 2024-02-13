# -*- coding: utf-8 -*-
"""
Created on Sat Dec 29 00:55:39 2018

@author: Vidya Dinesh
"""
#--------------------------------------------------------------------------------
class Properties(object):
    """Properties - Key Value Pair"""
    
    def __init__(self, key, value):
        self.Key = key
        self.Value = value
        
#--------------------------------------------------------------------------------      
class DataType(object):
    """Class used to store Datatype and its property""" 
    #datatype = ""
    #prop = [{'Key': None, 'Value': True}]
   
    def __init__(self, value, propty=None, dummy=None):
       self.datatype = value
       if propty:
           self.prop = propty
       else:
           self.prop = None
           #self.prop = [{'Key': None, 'Value': True}]
       
#--------------------------------------------------------------------------------      
class Date(object):
    """Date parameters of a Data Request"""
    #Start = ""
    #End = ""
    #Frequency = ""
    #Kind = 0
    
    def __init__(self, startDate = "", freq = "D", endDate = "", kind = 0):
       self.Start = startDate
       self.End = endDate
       self.Frequency = freq
       self.Kind = kind

#--------------------------------------------------------------------------------                  
class Instrument(Properties):
    """Instrument and its Properties"""
    #instrument = ""
    properties = [Properties]
    
    def __init__(self, inst, props):
        self.instrument = inst
        self.properties = props
    
#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
"""Classes that help to form the Request in RAW JSON format"""
class TokenRequest(Properties):
    #password = ""
    #username = ""
    
    def __init__(self, uname, pword, propties = None):
        self.username = uname
        self.password = pword
        self.properties = propties
        
    def get_TokenRequest(self):
        tokenReq = {"Password":self.password,"Properties":[],"UserName":self.username}
        props =[{'Key': eachPrpty.Key,'Value':eachPrpty.Value} for eachPrpty in self.properties] if self.properties else None 
        tokenReq["Properties"] = props
        return tokenReq
#--------------------------------------------------------------------------------
class DataRequest:
     
    hints = {'E':'IsExpression', 'L':'IsList', 
             'N':'ReturnName', 'C':'ReturnCurrency'}

    singleReq = dict
    multipleReqs = dict
    
    def __init__(self):
        self.singleReq = {"DataRequest":{},"Properties":None,"TokenValue":""}
        self.multipleReqs = {"DataRequests":[],"Properties":None,"TokenValue":""}
    
    def get_bundle_Request(self, reqs, token=""):
        self.multipleReqs["DataRequests"] = []
        for eachReq in reqs:
            dataReq = {"DataTypes":[],"Instrument":{}, "Date":{}, "Tag":None}
            dataReq["DataTypes"] = self._set_Datatypes(eachReq[0]["DataTypes"])
            dataReq["Date"] = self._set_Date(eachReq[0]["Date"])
            dataReq["Instrument"] = self._set_Instrument(eachReq[0]["Instrument"])
            self.multipleReqs["DataRequests"].append(dataReq)
            
        self.multipleReqs["Properties"] = None
        self.multipleReqs["TokenValue"] = token
        return self.multipleReqs
        
        
    def get_Request(self, req, token=""):
        dataReq = {"DataTypes":[],"Instrument":{}, "Date":{}, "Tag":None}
        dataReq["DataTypes"] = self._set_Datatypes(req["DataTypes"])
        dataReq["Date"] = self._set_Date(req["Date"])
        dataReq["Instrument"] = self._set_Instrument(req["Instrument"])
        self.singleReq["DataRequest"] = dataReq
        
        self.singleReq["Properties"] = None
        self.singleReq["TokenValue"] = token
        return self.singleReq
    
#--------------------HELPER FUNCTIONS--------------------------------------      
    def _set_Datatypes(self, dtypes=None):
        """List the Datatypes"""
        datatypes = []
        for eachDtype in dtypes:
            if eachDtype.datatype == None:
                continue
            else:
                datatypes.append({"Properties":eachDtype.prop, "Value":eachDtype.datatype})
        return datatypes
            
        
    def _set_Instrument(self, inst):
        propties = [{'Key': DataRequest.hints[eachPrpty.Key],'Value': True} 
                for eachPrpty in inst.properties] if inst.properties else None
        return {"Properties": propties, "Value": inst.instrument}
        
    def _set_Date(self, dt):
        return {"End":dt.End,"Frequency":dt.Frequency,"Kind":dt.Kind,"Start":dt.Start}


 #--------------------------------------------------------------------------        
    
   

            
    
##Datatypes
#dat =[]
#dat.append(DataType("PH"))
#dat.append(DataType("PL"))
#dat.append(DataType("P"))
##Instrument
#Props = [IProperties("E", True)]
#ins = Instrument("VOD", Props)
#ins2 = Instrument("U:F", Props)
##Date
#dt = Date(startDate = "20180101",freq= "M",kind = 1)
#
#dr = DataRequest()
#req1 = {"DataTypes":dat,"Instrument":ins,"Date":dt}
#req2 = {"DataTypes":dat,"Instrument":ins2,"Date":dt}
#datareq = dr.get_Request(req=req1, source='PROD',token='token')
#print(datareq)




    
    


    
    
        
        
    

