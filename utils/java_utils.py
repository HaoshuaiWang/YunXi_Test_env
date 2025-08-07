import requests
def  executeCaPostProcessing(modelId,taskCode,taskType):
    url = "http://10.243.96.46:9003/flood-control-api//urbanFlood/floodManager/executeCaPostProcessing"
    params = {
        "modelId": modelId,
        "taskCode": taskCode,
        "taskType": taskType
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    print(f"调用 Java 接口成功，响应: {response.text}")

if __name__ == "__main__":
    modelId= "A4_420322_YX",
    taskCode="c20782e7ec67451e9531ab54270bed9e",
    taskType= "telemac"
    executeCaPostProcessing(modelId,taskCode,taskType)
