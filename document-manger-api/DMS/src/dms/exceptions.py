class DataStoryException(Exception):
    """Base exception for all data story related errors"""
    pass

class ExtractionError(DataStoryException):
    """Raised when data extraction fails"""
    pass

class AnalysisError(DataStoryException):
    """Raised when data analysis fails"""
    pass

class VisualizationError(DataStoryException):
    """Raised when visualization creation fails"""
    pass

class InvalidQueryError(DataStoryException):
    """Raised when the input query is invalid"""
    pass

class ODataServiceError(DataStoryException):
    """Raised when OData service encounters an error"""
    pass 