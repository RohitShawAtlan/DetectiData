import logging
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, Table, Connection
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.fields.atlan_fields import AtlanField
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.search import IndexSearchRequest
from pyatlan.model.core import Announcement
from pyatlan.model.enums import AtlanConnectorType, AnnouncementType
from pyatlan.model.lineage import FluentLineage
from pyatlan.model.enums import AtlanConnectorType, LineageDirection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
issue_asssets={}
def search_tables(client,connection,database_name,schema_name,name):
    request = FluentSearch(
        wheres=[
            Asset.TYPE_NAME.eq("Table"),
            Table.QUALIFIED_NAME.startswith(connection.qualified_name),
            Table.DATABASE_NAME.eq(database_name, True),
            Table.SCHEMA_NAME.eq(schema_name, True),
            Table.NAME.eq(name, True)
        ]
    ).to_request()
    tables = client.asset.search(request)
    return tables

def update_metadata(client,table,row):
    update = table.trim_to_required()
    cma = update.get_custom_metadata("DetectiData Table Tracker")
    cma["Trust Score"] = row[2]
    cma["Accuracy"] = row[3]
    cma["Completeness"] = row[4]
    cma["Uniqueness"] = row[5]
    cma["Validity"] = row[6]
    

    response = client.asset.save_merging_cm(update)
    result = response.assets_updated(Table)[0]

    if(int(row[2])<50):
        handle_announcement(client,result.guid,"issue")
    elif(int(row[2])<80):
        handle_announcement(client,result.guid,"warning")
    logger.info(f"Result: {result}")

def get_directly(qualifier_name, client):

    table = client.asset.get_by_qualified_name(
            f"{qualifier_name}"
        )
    return table

def handle_announcement(client,guid,tag):
    request = FluentLineage(
            starting_guid=guid,
            direction=LineageDirection.DOWNSTREAM,
            # where_assets=[FluentLineage.ACTIVE],
            # includes_in_results=[Asset.TYPE_NAME.in_lineage.neq("Process")],
            includes_on_results=Asset.GUID
        ).request
    request.immediate_neighbors = True
    response = client.asset.get_lineage_list(request)
    count = 0
    for asset in response:
        try:
            logger.info(
                f"{asset.qualified_name} -> "
                # f"{[(x.qualified_name,x.name )for x in asset.immediate_downstream] if asset.immediate_downstream else []}"
                f"{[x.qualified_name for x in asset.immediate_downstream] if asset.immediate_downstream else []}"
            )
            add_annoucement(client,asset,tag)
            if(tag=="issue"):
                issue_asssets[asset.guid]=(asset,tag)
            elif(tag=="warning"):
                if(issue_asssets[asset.guid]):
                    pass
                else:
                    issue_asssets[asset.guid]=(asset,tag)
                
            # for x in asset.immediate_downstream:
            #     if asset.immediate_downstream:
            #         add_annoucement(client,x.qualified_name,tag)
            #         handle_announcement(client,x.guid,tag)
            count += 1
        except:
            print("Error \n\n")
    logger.info(f"Found {count} downstream assets (not including processes).")

def add_annoucement(client,asset,tag):
    # table  = get_directly(qualifier_name,client)
    message=""
    if(tag=="warning"):
        message= "Trust Score is less than 80%"
        asset.set_announcement(Announcement(
            announcement_type=AnnouncementType.WARNING,
            announcement_title=tag,
            announcement_message=message
        ))
    else:
        message = "Trust Score is less than 50%"
        asset.set_announcement(Announcement(
                announcement_type=AnnouncementType.ISSUE,
                announcement_title=tag,
                announcement_message=message
            ))
    response = client.asset.save(asset)
    # result = response.assets_created(Table)[0]
    # logger.info(f"Result: {result}")
    # return result.guid