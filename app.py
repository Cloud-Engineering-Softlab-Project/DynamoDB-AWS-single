# from aws_xray_sdk.ext.flask.middleware import XRayMiddleware
# from aws_xray_sdk.core import xray_recorder, patch_all

import configuration
app = configuration.init()

from resources.energy_data import EnergyData, ReferenceZones
configuration.api.add_resource(EnergyData, '/energy_data')
configuration.api.add_resource(ReferenceZones, '/ref_zones')

# xray_recorder.configure(service='Energy APIs')

# Instrument the Flask application
# XRayMiddleware(app, xray_recorder)

# patch_all()

# We only need this for local development.
if __name__ == '__main__':
    configuration.app.run(debug=True)
