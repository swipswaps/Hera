import xarray 
import pandas 
from unum.units import *

class TBmaxperiod(object):
	"""
		Init : 
			n  - The TB constant. 
			SW - The sampling window 
		
	
		Calculate the 
			max over time of (C_{SW mean}**n*SW mean). 
	 
	"""
	_n = None

	_Qunit = None

	@property 
	def n(self):
		return self._n

	@property 
	def period(self):
		return self._period

	@period.setter 
	def period(self,value):
		self._period = value 


	def __init__(self,n,dt,Qunit=1*mg):
		self._n = n 
		self._Qunit = Qunit
		self._dt = dt

	def getTB(self,data,fieldname,period):
		"""
			Get the Ten-Berge calculation for the data field.

			average the concentrations over the priod

		:param data: 		xarray with the data
		:param fieldname:   the field in the xarray to use
		:param period: 		the number of dt to use in the averaging.
		:return: the max TB dose for each point.
		"""
		newC = data[fieldname].rolling(datetime=period).mean()  #.max(dim="datetime")
		Cunit = (self._Qunit/m**3)**self._n
		TL   = (newC**self.n)*(self._dt*period*(Cunit)).asNumber(Cunit*min).max(dim="datetime")
		return TL
		
