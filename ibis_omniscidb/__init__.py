"""OmniSciDB backend."""
from typing import Optional

from ibis.backends.base import BaseBackend
from ibis.backends.base.client import Database

from .client import OmniSciDBClient, OmniSciDBTable

__all__ = ('Backend',)


class Backend(BaseBackend):
    """When the backend is loaded, this class becomes `ibis.omniscidb`."""

    # TODO Subclass `BaseSQLBackend` instead of `BaseBackend, after this:
    # https://github.com/ibis-project/ibis/pull/2864

    name = 'omniscidb'
    kind = 'sql'  # TODO also remove with #2864
    client_class = OmniSciDBClient
    database_class = Database
    table_expr_class = OmniSciDBTable

    def connect(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = 6274,
        database: Optional[str] = None,
        protocol: str = 'binary',
        session_id: Optional[str] = None,
        ipc: Optional[bool] = None,
        gpu_device: Optional[int] = None,
    ):
        """Create a client for OmniSciDB backend.

        Parameters
        ----------
        uri : str, optional
        user : str, optional
        password : str, optional
        host : str, optional
        port : int, default 6274
        database : str, optional
        protocol : {'binary', 'http', 'https'}, default 'binary'
        session_id: str, optional
        ipc : bool, optional
          Enable Inter Process Communication (IPC) execution type.
          `ipc` default value is False when `gpu_device` is None, otherwise
          its default value is True.
        gpu_device : int, optional
          GPU Device ID.

        Returns
        -------
        OmniSciDBClient
        """
        return OmniSciDBClient(
            backend=self,
            uri=uri,
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            protocol=protocol,
            session_id=session_id,
            ipc=ipc,
            gpu_device=gpu_device,
        )
