__author__ = 'ekampf'

import textwrap


class BigQueryError(Exception):


    # pylint: disable=R0911
    @staticmethod
    def create(error, server_error, error_ls, job_ref=None):
        """Returns a BigQueryError for the JSON error that's embedded in the server error response.

        If error_ls contains any errors other than the given one, those are also included in the
        return message.

        :param error: The primary error to convert.
        :param server_error: The error returned by the server. (Only used in case error is malformed.)
        :param error_ls: Additional errors included in the error message.
        :param job_ref: A job reference if its an error associated with a job.
        :return:
          A BigQueryError instance.
        """


        reason = error.get('error', None) or error.get('reason', None)
        if job_ref:
            message = 'Error processing %r: %s' % (job_ref, error.get('message'))
        else:
            message = error.get('message')


        new_errors = [err for err in error_ls if err != error]
        if new_errors:
            message += '\nFailure details:\n'
            message += '\n'.join(textwrap.fill(': '.join(filter(None, [err.get('location', None), err.get('message', '')])), initial_indent=' - ', subsequent_indent='   ') for err in new_errors)

        if not reason or not message:
            return BigQueryInterfaceError('Error reported by server with missing error fields. ' 'Server returned: %s' % (str(server_error),))

        if reason == 'authError':
            return BigQueryAuthorizationError(message)

        # BigQueryServiceError derivatives

        if reason == 'notFound':
            return BigQueryNotFoundError(message, error, error_ls, job_ref=job_ref)
        if reason == 'duplicate':
            return BigQueryDuplicateError(message, error, error_ls, job_ref=job_ref)
        if reason == 'accessDenied':
            return BigQueryAccessDeniedError(message, error, error_ls, job_ref=job_ref)
        if reason == 'invalidQuery':
            return BigQueryInvalidQueryError(message, error, error_ls, job_ref=job_ref)
        if reason == 'termsOfServiceNotAccepted':
            return BigQueryTermsOfServiceError(message, error, error_ls, job_ref=job_ref)
        if reason == 'backendError':
            return BigQueryBackendError(message, error, error_ls, job_ref=job_ref)

        # We map the less interesting errors to BigQueryServiceError.
        return BigQueryServiceError(message, error, error_ls, job_ref=job_ref)

class BigQueryAuthorizationError(BigQueryError):
    """401 error wrapper"""
    pass

class BigQueryCommunicationError(BigQueryError):
    """Error communicating with the server."""
    pass


class BigQueryInterfaceError(BigQueryError):
    """Response from server missing required fields."""
    pass


class BigQueryServiceError(BigQueryError):
    """Base class of BigQuery-specific error responses.

    The BigQuery server received request and returned an error.
    """

    def __init__(self, message, error, error_list, job_ref=None, *args, **kwds):
        """Initializes a BigQueryServiceError.

        :param message: A user-facing error message.
        :param error: The error dictionary, code may inspect the 'reason' key.
        :param error_list: A list of additional entries, for example a load job may contain multiple errors here for each error encountered during processing.
        :param job_ref: Optional job reference.
        :return:
            A BigQueryError instance.
        """
        super(BigQueryServiceError, self).__init__(message, *args, **kwds)
        self.error = error
        self.error_list = error_list
        self.job_ref = job_ref

    def __repr__(self):
        return '%s: error=%s, error_list=%s, job_ref=%s' % (self.__class__.__name__, self.error, self.error_list, self.job_ref)


class BigQueryNotFoundError(BigQueryServiceError):
    """The requested resource or identifier was not found."""
    pass


class BigQueryDuplicateError(BigQueryServiceError):
    """The requested resource or identifier already exists."""
    pass


class BigQueryAccessDeniedError(BigQueryServiceError):
    """The user does not have access to the requested resource."""
    pass


class BigQueryInvalidQueryError(BigQueryServiceError):
    """The SQL statement is invalid."""
    pass


class BigQueryTermsOfServiceError(BigQueryAccessDeniedError):
    """User has not ACK'd ToS."""
    pass


class BigQueryBackendError(BigQueryServiceError):
    """A backend error typically corresponding to retriable HTTP 503 failures."""
    pass


class BigQueryClientError(BigQueryError):
    """Invalid use of BigQueryClient."""
    pass


class BigQueryClientConfigurationError(BigQueryClientError):
    """Invalid configuration of BigQueryClient."""
    pass


class BigQuerySchemaError(BigQueryClientError):
    """Error in locating or parsing the schema."""
    pass

class BigQueryStreamingMaximumRowSizeExceededError(BigQueryError):
    """Thrown when trying to stream-insert a row thats too big."""
    pass


