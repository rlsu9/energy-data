#!/usr/bin/env python3

from flask import Flask
from flask_restful import Api
import webargs

from resources.balancing_authority import BalancingAuthority


app = Flask(__name__)
api = Api(app)

api.add_resource(BalancingAuthority, '/balancing-authority/')

# Source: https://github.com/marshmallow-code/webargs/issues/181#issuecomment-621159812
@webargs.flaskparser.parser.error_handler
def webargs_validation_handler(error, req, schema, *, error_status_code, error_headers):
    """Handles errors during parsing. Aborts the current HTTP request and
    responds with a 422 error.
    """
    status_code = error_status_code or webargs.core.DEFAULT_VALIDATION_STATUS
    webargs.flaskparser.abort(
        status_code,
        exc=error,
        messages=error.messages,
    )

if __name__ == '__main__':
    app.run(debug=True)
