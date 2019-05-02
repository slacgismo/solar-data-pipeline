"""
This module contains Model (as in class to contain data).
"""
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import UserDefinedType
from cassandra.cqlengine.usertype import UserType

class Geopoint(UserType):
    __keyspace__ = 'measurements'
    latitude = columns.Double()
    longitude = columns.Double()

class MeasurementRaw(Model):
    __keyspace__ = 'measurements'
    site = columns.Text(required=True, primary_key=True)
    meas_name = columns.Text(required=True, primary_key=True)
    ts = columns.DateTime(primary_key=True) # timestamp in schema
    sensor = columns.Text(required=True, primary_key=True)
    station = columns.Text(required=True, primary_key=True)
    company = columns.Text(required=True, primary_key=True)
    lat_lon = UserDefinedType(Geopoint)
    meas_description = columns.Text()
    meas_status = columns.Boolean()
    meas_unit = columns.Text()
    meas_val_b = columns.Boolean()
    meas_val_f = columns.Float()
    meas_val_s = columns.Text()
