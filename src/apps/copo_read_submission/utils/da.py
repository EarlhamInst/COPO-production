from common.dal.copo_da import DAComponent

class SubmissionQueue(DAComponent):
    def __init__(self, profile_id=None):
        super(SubmissionQueue, self).__init__(profile_id, "submissionQueue")
