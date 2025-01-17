from . import Databricks

class NameTypeNotSupportedException: pass

class ReturnTypeNotSupportedException: pass

class Groups(Databricks.Databricks):
	def __init__(self, url, token=None):
		super().__init__(token)
		self._api_type = 'groups'
		self._url = url

	def addMember(self, parent_name, name, name_type):
		endpoint = 'add-member'
		url = self._set_url(self._url, self._api_type, endpoint)

		if name_type.lower() == 'user':
			payload = {
				'user_name': name,
			}
		elif name_type.lower() == 'group':
			payload = {
				'group_name': name
			}
		else:
			raise NameTypeNotSupportedException("The name type '{}' is not supported. Please use either 'user' or 'group' name_types.".format(name_type))

		payload['parent_name'] = parent_name

		return self._post(url, payload)

	def createGroup(self, group_name):
		endpoint = 'group'
		url = self._set_url(self._url, self._api_type, endpoint)

		payload = {
			'group_name': group_name

		}

		return self._post(url, payload)

	
	def listGroupMembers(self, group_name, return_type='json'):
		endpoint = 'list-members?group_name='+group_name
		url = self._set_url(self._url, self._api_type, endpoint)

		r =  self._get(url)

		if return_type.lower() == 'json':
			return r
		elif return_type.lower() == 'list':
			return [user['user_name'] for user in r['members']]
		else:
			raise ReturnTypeNotSupportedException("The return type '{}' is not supported. Please use either 'json' or 'list'".format(return_type))

	def listGroups(self):
		endpoint = 'list'
		url = self._set_url(self._url, self._api_type, endpoint)
		
		return self._get(url)

	def listParents(self, name, name_type):

		if name_type.lower() == 'user':
			endpoint = 'list-parents?user_name='+name
		elif name_type.lower() == 'group':
			endpoint = 'list-parents?group_name='+name
		else:
			raise NameTypeNotSupportedException("The name type '{}' is not supported. Please use either 'user' or 'group' name_types.".format(name_type))

		url = self._set_url(self._url, self._api_type, endpoint)

		return self._get(url)

	def removeMember(self, name, parent_name, name_type):
		endpoint = 'remove-member'
		url = self._set_url(self._url, self._api_type, endpoint)

		if name_type.lower() == 'user':
			payload = {
				'user_name': name,
			}
		elif name_type.lower() == 'group':
			payload = {
				'group_name': name
			}
		else:
			raise NameTypeNotSupportedException("The name type '{}' is not supported. Please use either 'user' or 'group' name_types.".format(name_type))

		payload['parent_name'] = parent_name

		return self._post(url, payload)

	def deleteGroup(self, group_name):
		endpoint = 'delete'
		url = self._set_url(self._url, self._api_type, endpoint)

		payload = {
			'group_name': group_name
		}

		return self._post(url, payload)



