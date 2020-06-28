from .utils import _init_session
from .login import Login

class Base():
    def __init__(self, **kwargs):
        self.driver = None
        self.session = _init_session(kwargs.pop('session', None), **kwargs)
        self.crumb = kwargs.pop('crumb', None)
        if kwargs.get('username') and kwargs.get('password'):
            self.login(kwargs.get('username'), kwargs.get('password'))


    def login(self, username, password):
        yf_login = Login(username, password)
        self.driver = yf_login.get_driver()
        d = yf_login.get_cookies()
        try:
            [self.session.cookies.set(c['name'], c['value'])
                for c in d['cookies']]
            self.crumb = d['crumb']
        except TypeError:
            print('Invalid credentials provided.  Please check username and'
                    ' password and try again')