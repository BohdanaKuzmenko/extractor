#!flask/bin/python

from app import app

if __name__ == '__main__':
    app.run(host='192.168.1.43', debug=True, threaded=True)
