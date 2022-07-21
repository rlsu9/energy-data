#!/usr/bin/env python3

import time
from flask import Flask, g
from flask_restful import Api
import webargs
import secrets
import json
import logging

from api.util import json_serialize, logger
json.JSONEncoder.default = json_serialize

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = secrets.token_hex()
    if __name__ != '__main__':
        # Source: https://trstringer.com/logging-flask-gunicorn-the-manageable-way/
        gunicorn_logger = logging.getLogger('gunicorn.error')
        logger.addHandler(gunicorn_logger)
        logger.setLevel(gunicorn_logger.level)

    from api.resources.balancing_authority import BalancingAuthority, BalancingAuthorityList
    from api.resources.carbon_intensity import CarbonIntensity
    from api.resources.carbon_aware_scheduler import CarbonAwareScheduler

    api = Api(app)
    api.add_resource(BalancingAuthority, '/balancing-authority/')
    api.add_resource(BalancingAuthorityList, '/balancing-authority/list')
    api.add_resource(CarbonIntensity, '/carbon-intensity/')
    api.add_resource(CarbonAwareScheduler, '/carbon-aware-scheduler/')

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

    @app.before_request
    def before_request():
        g.start = time.time()

    @app.teardown_request
    def teardown_request(exception=None):
        diff = time.time() - g.start
        logger.debug(f'Request took {1000 * diff:.0f}ms')

    return app
