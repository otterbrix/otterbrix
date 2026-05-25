from typing import Optional
import otterbrix
from otterbrix import OtterBrixPyConnection

from otterbrix.experimental.spark.exception import ContributionsAcceptedError
from otterbrix.experimental.spark.conf import SparkConf


class SparkContext:

    _active_spark_context = None

    def __init__(self, master: str):
        self._connection = otterbrix.connect('default')
        SparkContext._active_spark_context = self.connection

    def __del__(self):
        _active_spark_context = None
    
    @property
    def connection(self) -> OtterBrixPyConnection:
        return self._connection

    def stop(self) -> None:
        _active_spark_context = None
        self._connection.close()

    @classmethod
    def getOrCreate(cls, conf: Optional[SparkConf] = None) -> "SparkContext":
        raise ContributionsAcceptedError

    @classmethod
    def setSystemProperty(cls, key: str, value: str) -> None:
        raise ContributionsAcceptedError

    @property
    def applicationId(self) -> str:
        raise ContributionsAcceptedError

    @property
    def defaultMinPartitions(self) -> int:
        raise ContributionsAcceptedError

    @property
    def defaultParallelism(self) -> int:
        raise ContributionsAcceptedError

    @property
    def startTime(self) -> str:
        raise ContributionsAcceptedError

    @property
    def uiWebUrl(self) -> str:
        raise ContributionsAcceptedError

    @property
    def version(self) -> str:
        raise ContributionsAcceptedError

    def __repr__(self) -> str:
        raise ContributionsAcceptedError

    def addArchive(self, path: str) -> None:
        raise ContributionsAcceptedError

    def addFile(self, path: str, recursive: bool = False) -> None:
        raise ContributionsAcceptedError

    def addPyFile(self, path: str) -> None:
        raise ContributionsAcceptedError

    def cancelAllJobs(self) -> None:
        raise ContributionsAcceptedError

    def cancelJobGroup(self, groupId: str) -> None:
        raise ContributionsAcceptedError

    def dump_profiles(self, path: str) -> None:
        raise ContributionsAcceptedError

    def getCheckpointDir(self) -> Optional[str]:
        raise ContributionsAcceptedError

    def getConf(self) -> SparkConf:
        raise ContributionsAcceptedError

    def getLocalProperty(self, key: str) -> Optional[str]:
        raise ContributionsAcceptedError

    def setCheckpointDir(self, dirName: str) -> None:
        raise ContributionsAcceptedError

    def setJobDescription(self, value: str) -> None:
        raise ContributionsAcceptedError

    def setJobGroup(self, groupId: str, description: str, interruptOnCancel: bool = False) -> None:
        raise ContributionsAcceptedError

    def setLocalProperty(self, key: str, value: str) -> None:
        raise ContributionsAcceptedError

    def setLogLevel(self, logLevel: str) -> None:
        raise ContributionsAcceptedError

    def show_profiles(self) -> None:
        raise ContributionsAcceptedError

    def sparkUser(self) -> str:
        raise ContributionsAcceptedError



__all__ = ["SparkContext"]
