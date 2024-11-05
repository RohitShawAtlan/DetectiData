import logging
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, Table, Connection
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.fields.atlan_fields import AtlanField
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.search import IndexSearchRequest

def get_connection(name):
    client = AtlanClient()
    # TODO: Replace with YOUR connection's name
    connection = client.asset.find_connections_by_name(name, AtlanConnectorType.MSSQL)[0]
    return connection,client