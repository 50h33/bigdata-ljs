# coding: utf-8
import os
import mylib


def doIt():
    keyPath = os.path.join(os.getcwd(), 'src', 'key.properties')
    key = mylib.getKey(keyPath)
    print(key['dataseoul'])


if __name__ == '__main__':
    doIt()
