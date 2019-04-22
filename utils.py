import hmac
import base64
import hashlib
import datetime
import dateutil.tz

from requests.auth import AuthBase

def xcode(text, encoding='utf8', mode='ignore'):
    return text.encode(encoding, mode)

class HmacAuth(AuthBase):
    '''
    Adapted from https://github.com/bazaarvoice/python-hmac-auth
    '''

    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

    def __call__(self, request):
        ts = datetime.datetime.now(dateutil.tz.tzutc()).isoformat() # ISO8601

        msg = [self.api_key, request.method, ts, request.path_url]
        if request.body:
            msg.append(request.body)
        msg = '\n'.join(msg)

        digest = hmac.new(bytes(self.secret_key,'UTF-8'), bytes(msg, 'utf-8'), hashlib.sha256).digest()
        sig = str(base64.standard_b64encode(digest).strip(), 'utf-8')

        request.headers['Authorization'] = 'HMAC %s:%s' % (self.api_key, sig)
        request.headers['X-Auth-Timestamp'] = ts

        return request

class PY2HmacAuth(AuthBase):
    '''
    Adapted from https://github.com/bazaarvoice/python-hmac-auth
    '''

    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

    def __call__(self, request):
        ts = datetime.datetime.now(dateutil.tz.tzutc()).isoformat() # ISO8601

        msg = [self.api_key, request.method, ts, request.path_url]
        if request.body:
            msg.append(str(request.body))
        msg = '\n'.join(msg)

        digest = hmac.new(xcode(self.secret_key,'UTF-8'), xcode(msg, 'utf-8'), hashlib.sha256).digest()
        sig = str(base64.standard_b64encode(digest).strip(), 'utf-8')

        request.headers['Authorization'] = 'HMAC %s:%s' % (self.api_key, sig)
        request.headers['X-Auth-Timestamp'] = ts

        return request

