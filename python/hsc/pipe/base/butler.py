from lsst.pipe.base import TaskRunner

class ButlerTaskRunner(TaskRunner):
    """Get a butler into the Task scripts"""
    @staticmethod
    def getTargetList(parsedCmd, **kwargs):
        """MpiTask.run methods should receive a butler in the kwargs"""
        return TaskRunner.getTargetList(parsedCmd, butler=parsedCmd.butler, **kwargs)


def getDataRef(butler, dataId):
    """Construct a dataRef from a butler and data identifier"""
    dataRefList = [ref for ref in butler.subset(datasetType='raw', **dataId)]
    assert len(dataRefList) == 1
    return dataRefList[0]

