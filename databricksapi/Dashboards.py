from . import Databricks

class Dashboards(Databricks.Databricks):
	def __init__(self, url, token=None):
		super().__init__(token)
		self._url = url
		self._api_type = 'preview/sql'
		
	def listDashboards(self, page_size=None, page=None, order=None, q=None):
		
		if order and (order not in ("name", "created_at")): 
			return "Order by " + str(order) + " not supported"
			
		endpoint = 'dashboards?'
		if page_size: endpoint = endpoint + "page_size="+str(page_size) + "&"
		if page: endpoint = endpoint + "page=" + str(page) + "&"
		if order: endpoint = endpoint + "order="+order + "&"
		if q: endpoint = endpoint + "q="+q + "&"
		
		url = self._set_url(self._url, self._api_type, endpoint)

		return self._get(url)

	def cloneDashboard(self, dashboard_definition):
		endpoint = 'dashboards'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._post(url, dashboard_definition)
		
	def updateDashboard(self, dashboard_id, dashboard_definition):
		endpoint = 'dashboards/' + dashboard_id
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._post(url, dashboard_definition)
		
	def createDashboard(self, name, layout, dashboard_filters_enabled, widgets, is_trashed, is_draft, tags):
		endpoint = 'dashboards'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		payload = {
			"name": name
		}
		
		if layout: payload["layout"]=layout
		if dashboard_filters_enabled == True: 
			payload["dashboard_filters_enabled"]=True
		else:
			payload["dashboard_filters_enabled"]=False
		if widgets: payload["widgets"]=widgets
		if is_trashed == True: 
			payload["is_trashed"]=True
		else:
			payload["is_trashed"]=False
		if is_draft == True: 
			payload["is_draft"]=True
		else:
			payload["is_draft"]=False
		if tags: payload["tags"]=tags

		return self._post(url, payload)

	def getDashboard(self, dashboard_id):
		endpoint = 'dashboards/'+str(dashboard_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._get(url) 
		
	def getDashboardPermissions(self, dashboard_id):
		endpoint = 'permissions/dashboards/'+str(dashboard_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._get(url) 
		
	def updateDashboardPermissions(self, dashboard_id, acl):
		endpoint = 'permissions/dashboards/'+str(dashboard_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._post(url, acl) 
		
	def transferDashboard(self, dashboard_id, new_owner):
		endpoint = 'permissions/dashboard/'+str(dashboard_id)+'/transfer'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		payload = {"new_owner": new_owner}
		
		return self._post(url, payload) 

	def deleteDashboard(self, dashboard_id):
		endpoint = 'dashboards/'+str(dashboard_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._delete(url)  

	def restoreDashboard(self, dashboard_id):
		endpoint = 'dashboards/trash/'+str(dashboard_id)
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._post(url)

	def createWidget(self, widget_definition):
		endpoint = 'widgets'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._post(url, widget_definition)
		
	def createVisualization(self, vis_definition):
		endpoint = 'visualizations'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._post(url, vis_definition)
