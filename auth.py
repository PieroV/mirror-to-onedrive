#!/usr/bin/env python3
import client

LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize'


def login():
	cl = client.Client()
    oauth = cl.oauth
    oauth.redirect_uri = cl.config['redirect_uri']
    authorization_url, state = oauth.authorization_url(LOGIN_URL)
    code = input('Please go to \n{}\n and paste here the value of code: '
                 .format(authorization_url))
    token = oauth.fetch_token(client.TOKEN_URL, code=code,
                              client_secret=cl.config['client_secret'])
    cl.token_saver(token)


if __name__ == '__main__':
    login()
