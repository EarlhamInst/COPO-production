__author__ = 'felixshaw'

from .mongo_util import get_collection_ref
from bson.objectid import ObjectId
from bson.errors import InvalidId
from common.utils import helpers
from common.dal.mongo_util import cursor_to_list



class DataSchemas:
    ui_template_schemas = dict()
    schemas_collection_handler = get_collection_ref("Schemas")

    @classmethod
    def get_ui_template(cls, schema):
        if schema not in cls.ui_template_schemas:
            data = cls.schemas_collection_handler.find_one({"schemaName": schema.upper(), "schemaType": "UI"})
            if data:
                cls.ui_template_schemas[schema]= data.get("data", dict())
        return cls.ui_template_schemas.get(schema.upper(),[])

    @classmethod
    def add_ui_template(cls, schema, template):
        # remove any existing UI templates for the target schema
        cls.delete_ui_template(schema)
        doc = {"schemaName": schema.upper(), "schemaType": "UI", "data": template}
        cls.schemas_collection_handler.insert_one(doc)

    @classmethod
    def delete_ui_template(cls, schema):
        cls.schemas_collection_handler.delete_one({"schemaName": schema, "schemaType": "UI"})

    """
    @classmethod
    def get_ui_template(cls):
        try:
            doc = cls.Schemas_Collection_Handler.find_one({"schemaName": cls.Schema, "schemaType": "UI"})
            doc = doc["data"]
        except Exception as e:
            exception_message = "Couldn't retrieve component schema. " + str(e)
            print(exception_message)
            raise
        return doc
    """

    @classmethod
    def get_ui_template_node(cls, schema, identifier):
        doc = cls.get_ui_template(schema)
        #doc = {k.lower(): v for k, v in doc.items() if k.lower() == 'copo'}
        #doc = {k.lower(): v for k, v in doc.get("copo", dict()).items() if k.lower() == identifier.lower()}
        #return doc.get(identifier.lower(), dict()).get("fields", list())
        return  doc.get(schema.lower(), dict()).get(identifier.lower(),dict()).get("fields",[])
    
    @classmethod
    def refresh(cls):
        cls.ui_template_schemas = dict()


PubCollection = 'PublicationCollection'
PersonCollection = 'PersonCollection'
DataCollection = 'DataCollection'
SampleCollection = 'SampleCollection'
AuditCollection = 'AuditCollection'
SubmissionCollection = 'SubmissionCollection'
SourceCollection = 'SourceCollection'
DataFileCollection = 'DataFileCollection'
RemoteFileCollection = 'RemoteFileCollection'
DescriptionCollection = 'DescriptionCollection'
ProfileCollection = 'Profiles'
AnnotationReference = 'AnnotationCollection'
GroupCollection = 'GroupCollection'
RepositoryCollection = 'RepositoryCollection'
CGCoreCollection = 'CGCoreCollection'
TextAnnotationCollection = 'TextAnnotationCollection'
SubmissionQueueCollection = 'SubmissionQueueCollection'
MetadataTemplateCollection = 'MetadataTemplateCollection'
FileTransferQueueCollection = 'FileTransferQueueCollection'
StatsCollection = 'StatsCollection'
BarcodeCollection = 'BarcodeCollection'
ValidationQueueCollection = 'ValidationQueueCollection'
EnaFileTransferCollection = 'EnaFileTransferCollection'
APIValidationReport = 'ApiValidationReport'
TestCollection = 'TestCollection'
AssemblyCollection = 'AssemblyCollection'
AnnotationCollection = "SeqAnnotationCollection"
TaggedSequenceCollection = "TagSequenceCollection"
EnaChecklistCollection = "EnaChecklistCollection"
ReadObjectCollection = "SampleCollection"

handle_dict = dict(audit=get_collection_ref(AuditCollection),
                   publication=get_collection_ref(PubCollection),
                   person=get_collection_ref(PersonCollection),
                   sample=get_collection_ref(SampleCollection),
                   accessions=get_collection_ref(SampleCollection),
                   source=get_collection_ref(SourceCollection),
                   profile=get_collection_ref(ProfileCollection),
                   submission=get_collection_ref(SubmissionCollection),
                   datafile=get_collection_ref(DataFileCollection),
                   annotation=get_collection_ref(AnnotationReference),
                   group=get_collection_ref(GroupCollection),
                   repository=get_collection_ref(RepositoryCollection),
                   cgcore=get_collection_ref(CGCoreCollection),
                   textannotation=get_collection_ref(TextAnnotationCollection),
                   metadata_template=get_collection_ref(
                       MetadataTemplateCollection),
                   stats=get_collection_ref(StatsCollection),
                   test=get_collection_ref(TestCollection),
                   barcode=get_collection_ref(BarcodeCollection),
                   validationQueue=get_collection_ref(
                       ValidationQueueCollection),
                   enaFileTransfer=get_collection_ref(
                       EnaFileTransferCollection),
                   apiValidationReport=get_collection_ref(APIValidationReport),
                   assembly=get_collection_ref(AssemblyCollection),
                   seqannotation=get_collection_ref(AnnotationCollection),
                   submissionQueue=get_collection_ref(
                       SubmissionQueueCollection),
                   taggedseq=get_collection_ref(TaggedSequenceCollection),
                   enaChecklist=get_collection_ref(EnaChecklistCollection),
                   read=get_collection_ref(ReadObjectCollection)
                   )


class DAComponent:
    def __init__(self, profile_id=None, component=str()):
        self.profile_id = profile_id
        self.component = component

    def get_number(self):
        return self.get_collection_handle().count_documents({})

    def get_record(self, oid) -> object:
        """

        :rtype: object
        """
        doc = None
        handler = self.get_collection_handle()
        if handler is not None:
            try:
                doc = handler.find_one(
                    {"_id": ObjectId(oid)})
            except InvalidId as e:
                return e
        if not doc:
            pass

        return doc

    def get_records(self, oids: list) -> list:

        # return list of objects from the given oids list
        if not isinstance(oids, list):
            raise TypeError("Method requires a list")
        # make sure we have ObjectIds
        try:
            oids = list(map(lambda x: ObjectId(x), oids))
        except InvalidId as e:
            return e
        handler = self.get_collection_handle()
        if handler is not None:
            cursor = handler.find({"_id": {"$in": oids}})

        return cursor_to_list(cursor)

    def get_component_count(self):
        count = 0
        if self.get_collection_handle():
            count = self.get_collection_handle().count_documents(
                {'profile_id': self.profile_id, 'deleted': helpers.get_not_deleted_flag()})

        return count

    def get_collection_handle(self):
        if self.component in handle_dict:
            return handle_dict.get(self.component)
        else:
            None

    def get_id_base(self):
        base_dict = dict(
            publication="copo.publication",
            person="copo.person",
            datafile="copo.datafile",
            sample="copo.sample",
            accessions="copo.accessions",
            source="copo.source",
            profile="copo.profile",
            submission="copo.submission",
            repository="copo.repository",
            annotation="copo.annotation",
            assembly="copo.assembly",
            seqannotation="copo.seqannotation",
            investigation="i_",
            study="s_",
            assay="a_",
        )

        return base_dict.get(self.component, str())

    def get_qualified_field(self, elem=str()):
        return self.get_id_base() + "." + elem

    def get_schema(self, **kwargs):
        return dict(schema_dict=DataSchemas.get_ui_template_node("COPO", self.component))

    def get_component_schema(self, **kwargs):
        return DataSchemas.get_ui_template_node("COPO", self.component)

    def validate_record(self, auto_fields=dict(), validation_result=dict(), **kwargs):
        """
        validates record, could be overriden by sub-classes to perform component
        specific validation of a record before saving
        :param auto_fields:
        :param validation_result:
        :param kwargs:
        :return:  validation_result["status"]: "success", "error", "warning", validation_result["message"]
        """

        local_result = dict(status=validation_result.get("status", "success"),
                            message=validation_result.get("message", str()))

        return local_result

    def save_record(self, auto_fields=dict(), **kwargs):
        from common.schemas.utils import data_utils

        fields = dict()
        schema = kwargs.get("schema", list()) or self.get_component_schema()

        # set auto fields
        if auto_fields:
            fields = data_utils.DecoupleFormSubmission(
                auto_fields, schema).get_schema_fields_updated_dict()

        # should have target_id for updates and return empty string for inserts
        target_id = kwargs.pop("target_id", str())

        # set system fields
        system_fields = dict(
            date_modified=helpers.get_datetime(),
            deleted=helpers.get_not_deleted_flag()
        )

        if not target_id:
            system_fields["date_created"] = helpers.get_datetime()
            system_fields["profile_id"] = self.profile_id

        # extend system fields
        for k, v in kwargs.items():
            system_fields[k] = v

        # add system fields to 'fields' and set default values - insert mode only
        for f in schema:
            f_id = f["id"].split(".")[-1]
            try:
                v_id = f["versions"][0]
            except:
                v_id = ""
            if f_id in system_fields:
                fields[f_id] = system_fields.get(f_id)
            elif v_id in system_fields:
                fields[f_id] = system_fields.get(v_id)

            if not target_id and f_id not in fields:
                fields[f_id] = helpers.default_jsontype(f["type"])

        # if True, then the database action (to save/update) is never performed, but validated 'fields' are returned
        validate_only = kwargs.pop("validate_only", False)
        fields["date_modified"] = helpers.get_datetime()
        # check if there is attached profile then update date modified
        if "profile_id" in fields:
            self.update_profile_modified(fields["profile_id"])
        if validate_only is True:
            return fields
        else:
            if target_id:
                self.get_collection_handle().update_one(
                    {"_id": ObjectId(target_id)},
                    {'$set': fields})
            else:
                doc = self.get_collection_handle().insert_one(fields)
                target_id = str(doc.inserted_id)

            # return saved record
            rec = self.get_record(target_id)

            return rec

    def update_profile_modified(self, profile_id):
        handle_dict["profile"].update_one({"_id": ObjectId(profile_id)}, {
                                          "$set": {"date_modified": helpers.get_datetime()}})

    def get_all_records(self, sort_by='_id', sort_direction=-1, **kwargs):
        doc = dict(deleted=helpers.get_not_deleted_flag())
        if self.profile_id:
            doc["profile_id"] = self.profile_id

        return cursor_to_list(self.get_collection_handle().find(doc).sort([[sort_by, sort_direction]]))

    def get_all_records_columns(self, sort_by='_id', sort_direction=-1, projection=dict(), filter_by=dict()):
        filter_by["deleted"] = helpers.get_not_deleted_flag()
        if self.profile_id:
            filter_by["profile_id"] = self.profile_id

        return cursor_to_list(
            self.get_collection_handle().find(filter_by, projection).sort([[sort_by, sort_direction]]))

    def get_all_records_columns_server(self, sort_by='_id', sort_direction=-1, projection=dict(), filter_by=dict(),
                                       search_term=str(),
                                       limit=0, skip=0):

        filter_by["deleted"] = helpers.get_not_deleted_flag()

        # 'name' seems to be the only reasonable field to restrict searching; others fields are resolved
        filter_by["name"] = {'$regex': search_term, "$options": 'i'}

        if self.profile_id:
            filter_by["profile_id"] = self.profile_id

        if skip > 0:
            records = self.get_collection_handle().find(filter_by, projection).sort([[sort_by, sort_direction]]).skip(
                skip).limit(limit)
        else:
            records = self.get_collection_handle().find(filter_by, projection).sort([[sort_by, sort_direction]]).limit(
                limit)

        return cursor_to_list(records)

    def execute_query(self, query_dict=dict()):
        if self.profile_id:
            query_dict["profile_id"] = self.profile_id

        return cursor_to_list(self.get_collection_handle().find(query_dict))