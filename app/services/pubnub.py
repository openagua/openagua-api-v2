from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

# PubNub for canceling tasks (can we replace this with Celery?)
pnconfig = PNConfiguration()
pnconfig.subscribe_key = app.config.get('PUBNUB_SUBSCRIBE_KEY')
pnconfig.publish_key = app.config.get('PUBNUB_PUBLISH_KEY')
pnconfig.uuid = app.config.get('PUBNUB_UUID')
pnconfig.ssl = False
pubnub = PubNub(pnconfig)
