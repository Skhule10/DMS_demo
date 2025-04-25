import os
from fastapi import HTTPException, Security, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from functools import wraps
import json
import logging

logger = logging.getLogger(__name__)

LOCAL_ENV = os.getenv("ENV", "PROD").upper() == "LOCAL"
if not LOCAL_ENV:
        from sap import xssec
        from cfenv import AppEnv
           
class XSUAAMiddleware(HTTPBearer):
    def __init__(self, auto_error: bool = True, required_scopes: list = None):
        super(XSUAAMiddleware, self).__init__(auto_error=auto_error)
        self.env = AppEnv()
        self.xsuaa_service = self.env.get_service(label='xsuaa')
        self.required_scopes = required_scopes or []
        
    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(XSUAAMiddleware, self).__call__(request)
        
        if not credentials:
            raise HTTPException(status_code=403, detail="Invalid authorization code.")
        
        try:
            logger.debug("XSUAA Service credentials: %s", self.xsuaa_service.credentials)
            logger.debug("Request credentials: %s", credentials.credentials)
            
            # Verify JWT token with XSUAA
            security_context = xssec.create_security_context(
                credentials.credentials,
                self.xsuaa_service.credentials
            )           
           
            # Check required scopes
            if self.required_scopes:
                try:
                    # Try different methods to get scopes
                    try:
                        # Try to get scopes using has_scope method
                        user_scopes = []
                        for scope in self.required_scopes:
                            if security_context.check_scope(scope):
                                user_scopes.append(scope)
                        print("user_scopes",user_scopes)        
                    except AttributeError:
                        try:
                            # Try to get scopes from attributes
                            user_scopes = getattr(security_context, 'scope', [])
                            if isinstance(user_scopes, str):
                                user_scopes = user_scopes.split()
                        except AttributeError:
                            # Last resort: try to get from JWT claims
                            token_info = getattr(security_context, 'token_info', {})
                            user_scopes = token_info.get('scope', [])
                    
                    print("User scopes (raw):", user_scopes)
                    
                    # Convert scopes to list if it's not already
                    if isinstance(user_scopes, str):
                        user_scopes = user_scopes.split()
                    elif not isinstance(user_scopes, (list, tuple)):
                        user_scopes = list(user_scopes)
                    
                    print("User scopes (processed):", user_scopes)
                    
                    # Check if any required scope matches
                    has_required_scope = any(
                        required in user_scopes 
                        for required in self.required_scopes
                    )
                    
                    if not has_required_scope:
                        logger.warning(
                            "Insufficient permissions. Required: %s, Found: %s",
                            self.required_scopes,
                            user_scopes
                        )
                        raise HTTPException(
                            status_code=403,
                            detail="Insufficient permissions"
                        )
                except Exception as scope_error:
                    logger.error("Error checking scopes: %s", str(scope_error), exc_info=True)
                    print("Error checking scopes:", str(scope_error))
                    raise HTTPException(
                        status_code=500,
                        detail="Error checking permissions: " + str(scope_error)
                    )
            
            # Add security context to request state
            request.state.security_context = security_context
            return credentials.credentials            
       
        except Exception as e:
            logger.error("Authentication error: %s", str(e), exc_info=True)
            raise HTTPException(
                status_code=401,
                detail="Authentication failed: " + str(e)
            )

def requires_auth(*required_scopes: str):
    """Decorator to require specific scopes"""
    security = XSUAAMiddleware(required_scopes=required_scopes)
    return Security(security)

# Updated dependency functions
def get_current_user(token: str = Security(XSUAAMiddleware())):
    """Basic authentication check"""
    return token

def require_admin(token: str = requires_auth("$XSAPPNAME.Admin")):
    """Require admin scope"""
    return token

def require_write(token: str = requires_auth("$XSAPPNAME.Write")):
    """Require write scope"""
    return token

def require_read(token: str = requires_auth("$XSAPPNAME.Read")):
    """Require read scope"""
    return token 