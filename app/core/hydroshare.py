from flask import current_app as app
from hs_restclient import HydroShare, HydroShareAuthOAuth2


def download_from_hydroshare(res_id, res_type, res_username):

    client_id = app.config.get('HYDROSHARE_CLIENT_ID')
    client_secret = app.config.get('HYDROSHARE_CLIENT_SECRET')

    auth = HydroShareAuthOAuth2(client_id, client_secret)

    hs = HydroShare(auth=auth)

    resource = hs.getResource(res_id)

    return