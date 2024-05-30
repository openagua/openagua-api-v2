# Some notes on Google Earth Engine

The Hydrology module depends on Google Earth Engine (EE), which is difficult to setup. The main challenge is that documnetation is scattered, poor and/or out-of-date, and some things simply don't work as advertised. Furthermore, the APIs that support developing an app using EE are under development, so change often, rendering what scattered docs exist only somewhat useful.

Some places to start reading about development with EE include the links from the bottom of the EE development console:
* [App Engine & Earth Engine Overview](https://developers.google.com/earth-engine/app_engine_intro)
* [Service Accounts](https://developers.google.com/earth-engine/service_account) - An EE app such as OpenAgua's Hydrology blueprint needs a Service Account with Google to work. This is the main account that the app connects to for authentication and tracking.
* [App Engine Example Apps](https://developers.google.com/earth-engine/app_engine_examples)
* [Usage Limits](https://developers.google.com/earth-engine/usage)

## Authentication process

Use of EE functions begins with authentication to ensure the user (or app) is authorized to use EE. There are three basic approches: persistent authentication, service account key authentication, and client-side authentication in the context of a web app.

Persistent authentication, which presently works, is designed to authenticate a specific machine, and must be done manually. Because of this, it is not generally suitable for broad deployment in OpenAgua, as many end users may not be authenticated for EE.

Service Account key authentication is designed for server-to-server authentication, and is appropriate for deploying a web app, where machine-specific manual setup is not appropriate. However, as of writing, the Service Account key approach does not appear to work.

The third method is more appropriate for when an end user of a web app is requested to authenticate themselves for EE use. While this can help regulate against usage limats, it is not at all what we want for OpenAgua, so the method is not described here (see the [client-auth demo](https://github.com/google/earthengine-api/tree/master/demos/client-auth)).

### Persistent authentication

Steps to setting up a persistent authentication are as follows, taken primarily from [Google Earth Engine API Python installation instructions](https://developers.google.com/earth-engine/python_install):

1) Install the google-api-python-client:
```
pip install google-api-python-client
```

2) Install the Earth Engine Python API:
```
pip install earthengine-api
```

3) Set up authentication credentials (see [Setting Up Authentication Credentials](https://developers.google.com/earth-engine/python_install#setting-up-authentication-credentials))
```
earthengine authenticate
```
Now, follow the directions on screen. You should be taken automatically to a website for authentication, after which you will receive a code to paste into the console. This process will save an authentication token to the local machine.

4) That's it! Now you can use EE in your Python scripts by first authenticating with `ee.Initialize()`. When `ee.Initialize()` is called without arguments, a persistent authentication is assumed and the saved authentication token is read (in contrast, the Service Account key approach uses `ee.Initialize(credentials)`; see below).

### Service Account key authentication

**Note**: The following results in a deadend. Since it will hopefully work again in the future, and is needed for automated deployment of OpenAgua on servers, it is retained here as reference.

With the Service Account key approach, a private key is generated and used to authenticate the app with Google. The general logic here is consistent with the [server-auth demo](https://github.com/google/earthengine-api/tree/master/demos/server-auth), but that demo is quite out-of-date. To do this:

1) Create a new project at https://console.developers.google.com

2) Create a Google API Service Account for the project at http://console.developers.google.com/iam-admin/serviceaccounts. Click "CREATE SERVICE ACCOUNT" and during the creation process, also:
  i. Add roles "Project -> Owner" and "App Engine -> App Engine Admin" (not sure about this yet!)
  ii. "Furnish a new private key", and select "JSON". **Save this key!**

3) Use the Python Earth Engine API (`ee`) to authenticate/initialize as follows, using the Service Account JSON key:
```
import ee
from oauth2client.service_account import ServiceAccountCredentials
...
privatekey = '/path/to/privatekey.json'
EE_SCOPE = ee.oauth.SCOPE
credentials = ServiceAccountCredentials.from_json_keyfile_name(privatekey, scopes=EE_SCOPE)
ee.Initialize(credentials)
```
Note `ServiceAccountCredentials.from_json_keyfile_name` does not take the Service Account ID as an argument, since this is already stored in the JSON file.

Where this currently fails is in one of the two scopes defined in `ee.oauth.SCOPE`, https://www.googleapis.com/auth/earthengine and https://www.googleapis.com/auth/devstorage.full_control: only the latter works. Until this is resolved, the manual machine-based authorization approach must be used.
