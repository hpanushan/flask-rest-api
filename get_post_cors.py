from flask import jsonify

import logging

def get_post_cors(res,status_code):
    logging.info('post cors function')

    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.status_code = status_code

    return response

