import requests
import json
import os

class ZenodoClient:
    """
    A client for interacting with the Sapio API.

    Attributes:
        url (str): The URL of the Sapio API.
        api_token (str): The API token for authentication.
        guid (str): The GUID for the Sapio application.
        session (requests.Session): The session object for making HTTP requests.
    """

    def __init__(self):
        """
        Initializes a new instance of the SapioClient class.
        """
        self.url = os.getenv('ZENODOURL')
        self.api_token = os.getenv('ZENODOTOKEN')
        self.session = requests.Session()
        self.session.headers = {"Authorization": f"Bearer {self.api_token}"}
        
    def do_get(self, method, url_params={}):
        """
        Sends a GET request to the Sapio API.

        Args:
            method (str): The API method to call.
            params (dict): Optional parameters for the API method.

        Returns:
            dict: The JSON response from the API.

        Raises:
            Exception: If the API returns an error status code.
        """
        if method.startswith('/'):
            url = self.url + method
        else:
            url = self.url + '/' + method
        resp = self.session.get(url, params=url_params)
        print(url)
        if resp.status_code != 200:
            raise Exception('Error: ' + str(resp.status_code) + ' ' + resp.text)
        else:
            return json.loads(resp.text)
        
    def do_post(self, method, method_params={}, url_params={}, params=None, files=None):
        """
        Sends a POST request to the Sapio API.

        Args:
            method (str): The API method to call.
            method_params (dict): populate the method with the params.
            params (dict): Optional parameters for the API method.

        Returns:
            dict: The JSON response from the API.

        Raises:
            Exception: If the API returns an error status code.
        """
        final_method = method.format(**method_params)
        if method.startswith('/'):
            url = self.url + final_method
        else:
            url = self.url + '/' + final_method       
        print(url)
        resp = self.session.post(url=url, params=url_params, json=params, files=files )
        if  resp.status_code >= 400:
            raise Exception('Error: ' + str(resp.status_code) + ' ' + resp.text)
        else:
            return json.loads(resp.text)

    def do_put(self, method, method_params={}, url_params={}, params=None):
        """
        Sends a PUT request to the Sapio API.

        Args:
            method (str): The API method to call.
            _id (int): The ID of the record to update.
            params (dict): Optional parameters for the API method.

        Returns:
            dict: The JSON response from the API.

        Raises:
            Exception: If the API returns an error status code.
        """
        final_method = method.format(**method_params)
        if method.startswith('/'):
            url = self.url + final_method
        else:
            url = self.url + '/' + final_method
 
        print(url)  
        resp = self.session.put(url=url, params=url_params, json=params  )
        if resp.status_code >= 400:
            raise Exception('Error: ' + str(resp.status_code) + ' ' + resp.text)
        else:
            return json.loads(resp.text)

    def do_delete(self, method, method_params={}, url_params={}):
        """
        Sends a DELETE request to the Sapio API.

        Args:
            method (str): The API method to call.
            params (dict): Optional parameters for the API method.
            _id (int): The ID of the record to delete.

        Returns:
            dict: The JSON response from the API.

        Raises:
            Exception: If the API returns an error status code.
        """
        final_method = method.format(**method_params)
        if method.startswith('/'):
            url = self.url + final_method
        else:
            url = self.url + '/' + final_method
   
        print(url)  
        resp = self.session.delete(url=url, params=url_params)
        if resp.status_code >= 400:
            raise Exception('Error: ' + str(resp.status_code) + ' ' + resp.text)
        else:
            return True