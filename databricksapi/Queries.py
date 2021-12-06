from . import Databricks

class Queries(Databricks.Databricks):
	def __init__(self, url, token=None):
		super().__init__(token)
		self._url = url
		self._api_type = 'preview/sql'
		
	def listDataSources(self):
		endpoint = 'data_sources'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._get(url)
		
	def listQueries(self, page_size=None, page=None, order=None, q=None):
		
		if order and (order not in ("name", "created_at", "schedule", "runtime", "executed_at", "created_by")): 
			return "Order by " + str(order) + " not supported"
			
		endpoint = 'queries?'
		if page_size: endpoint = endpoint + "page_size="+str(page_size) + "&"
		if page: endpoint = endpoint + "page=" + str(page) + "&"
		if order: endpoint = endpoint + "order="+order + "&"
		if q: endpoint = endpoint + "q="+q + "&"
		
		url = self._set_url(self._url, self._api_type, endpoint)

		return self._get(url)

	def createQuery(self, data_source_id, query, name, description, schedule, options, visualizations):
		endpoint = 'queries'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		payload = {
			"data_source_id": data_source_id,
			"query": query,
			"name": name
		}
		
		if description: payload["description"]=description
		if schedule: payload["schedule"]=schedule
		if options: payload["options"]=options
		if visualizations: payload["visualizations"]=visualizations

		return self._post(url, payload)

	def getQuery(self, query_id):
		endpoint = 'queries/'+str(query_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._get(url) 

	def updateQuery(self, query_id, data_source_id=None, query=None, name=None, description=None, schedule=None, options=None, visualizations=None):
		endpoint = 'queries/'+str(query_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		payload = self.getQuery(query_id)

		if data_source_id: payload["data_source_id"]=data_source_id
		if query: payload["query"]=query
		if name: payload["name"]=name
		if description: payload["description"]=description
		if schedule: payload["schedule"]=schedule
		if options: payload["options"]=options
		if visualizations: payload["visualizations"]=visualizations
		
		return self._post(url, payload) 

	def deleteQuery(self, query_id):
		endpoint = 'queries/'+str(query_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._delete(url)  

	def restoreQuery(self, query_id):
		endpoint = 'queries/trash/'+str(query_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._post(url)
