from sapiopylib.rest.User import SapioUser
from sapiopylib.rest.DataMgmtService import DataMgmtServer
from sapiopylib.rest.DataRecordManagerService import DataRecordManager
from sapiopylib.rest.utils.recordmodel.RecordModelManager import RecordModelManager, RecordModelInstanceManager, \
    RecordModelRelationshipManager
from sapiopylib.rest.PicklistService import PicklistManager
from common.utils.helpers import  get_not_deleted_flag, get_env

SAPIO_USERNAME = get_env("SAPIO_USERNAME")  
SAPIO_PASSWORD = get_env("SAPIO_PASSWORD")
SAPIO_API_URL = get_env("SAPIO_API_URL")
class Sapio():
    sapio_user = SapioUser(url=SAPIO_API_URL,
                    guid=None, account_name="sapio",
                    username=SAPIO_USERNAME, password=SAPIO_PASSWORD)
    dataRecordManager: DataRecordManager = DataMgmtServer.get_data_record_manager(sapio_user)

    # Used to store and commit changes made to record models, also holds other managers
    rec_man: RecordModelManager = RecordModelManager(sapio_user)

    # Used to create new records and return the corresponding record models and convert data records to record models
    inst_man: RecordModelInstanceManager = rec_man.instance_manager

    # Used to modify parent/child relationships between records using record models
    relationship_man: RecordModelRelationshipManager = rec_man.relationship_manager 

    picklistManager: PicklistManager = DataMgmtServer.get_picklist_manager(sapio_user)   
    relationship_man = rec_man.relationship_manager