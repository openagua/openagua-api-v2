import requests

from app.config import Config

config = Config('development')


def test_send_mail():
    resp = requests.post(
        config.MAIL_API_ENDPOINT,
        auth=('api', config.MAIL_API_KEY),
        data={'from': f'OpenAgua tester <{config.MAIL_API_TEST_SENDER}>',
              'to': [config.MAIL_API_TEST_RECIPIENT],
              'subject': 'OpenAgua email test',
              'text': 'Hello, world!'})

    assert resp.status_code == 200
