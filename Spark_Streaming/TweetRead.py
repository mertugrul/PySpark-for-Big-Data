#!/usr/bin/env python
# coding: utf-8

import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""


class TweetListener(StreamListener):
    
    def __init__(self, c_socket):
        self.client_socket = c_socket
    
    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
        except BaseException as e:
            print("ERROR {}".format(e))
        return True
    
    def on_error(self, status):
        print(status)
        return True
            

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=['covid'])

if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 9991
    s.bind((host, port))
    
    print('listening on port {}'.format(port))
    
    s.listen(5)
    c, addr = s.accept()
    
    sendData(c)

