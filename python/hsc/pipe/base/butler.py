def getDataRef(butler, dataId):
    # ProcessCcdTask wants a dataRef, but they don't pickle so we need to reconstruct it from the dataId
    dataRefList = [ref for ref in butler.subset(datasetType='raw', **dataId)]
    assert len(dataRefList) == 1
    return dataRefList[0]
