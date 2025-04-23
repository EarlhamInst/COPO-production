class CopoRuntimeError(RuntimeError):

    def __init__(self, message, errors=dict()):

        # Call the base class constructor with the parameters it needs
        super(CopoRuntimeError, self).__init__(message)

        self.errors = errors
