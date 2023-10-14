from random import seed
import re
from types import NoneType
# from h11 import InformationalResponse
# from idna import check_bidi
import requests
import json
import xmltodict
from requests.sessions import Session
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
import logging
import time


def fileUpload():
    fileData = pd.read_excel('개체번호.xlsx', dtype='object')
    cowList = list(fileData['개체번호'])
    print('!!!fileUpload success!!!')
    print(f'업로드 개체수: {len(cowList)}')
    return cowList


def fileDownload(df1, df2):
    with pd.ExcelWriter('phenoCrlResult.xlsx') as writer:
        df1.to_excel(writer, sheet_name="phenoCrlResult", index=False)
        df2.to_excel(writer, sheet_name="crlerrorResult", index=False)


def requestsRetrySession(connect, read, backOffFactor):

    with Session() as session:

        RetryAfterStatusCodes = (400, 403, 500, 503)

        retry = Retry(
            total=(connect + read),
            connect=connect,
            read=read,
            backoff_factor=backOffFactor,
            status_forcelist=RetryAfterStatusCodes
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)

    return session


def issueNoRequestsConnect(session, key, cow):
    url = f"http://data.ekape.or.kr/openapi-data/service/user/grade/confirm/issueNo?animalNo={cow}&ServiceKey={key}"
    request = session.get(url)
    xpars = xmltodict.parse(request.text)
    jsonDump = json.dumps(xpars)
    jsonData = json.loads(jsonDump)
    return jsonData


def phenoRequestsConnect(session, key, issueNo, issueDate):
    url = f"http://data.ekape.or.kr/openapi-data/service/user/grade/confirm/cattle?issueNo={issueNo}&issueDate={issueDate}&ServiceKey={key}"
    request = session.get(url)
    xpars = xmltodict.parse(request.text)
    jsonDump = json.dumps(xpars)
    jsonData = json.loads(jsonDump)
    return jsonData


def farmInfoRequestsConnect(session, key, cow, option):
    url = f"http://data.ekape.or.kr/openapi-data/service/user/animalTrace/traceNoSearch?ServiceKey={key}&traceNo={cow}&optionNo={option}"
    request = session.get(url)
    xpars = xmltodict.parse(request.text)
    jsonDump = json.dumps(xpars)
    jsonData = json.loads(jsonDump)
    return jsonData


def resultCodeCheck(jsonData):

    resultCode = jsonData['response']['header']['resultCode']

    return resultCode


def retry(function, max_tries=10):
    global errorCount

    errorCount = 0

    for i in range(max_tries):
        time.sleep(1)
        print(f'url 연결을 재시도 합니다. retry:{i + 1}')
        jsonData = function
        resultCode = resultCodeCheck(jsonData)
        if resultCode == '00':
            break
        else:
            print(f'resultCode : {resultCode}')
            print(jsonData)
            errorCount += 1
            continue

    return jsonData


def valueExtraction(targetKey, targetJsonData):
    if targetKey in dict.keys(targetJsonData):
        globals()[f'{targetKey}'] = targetJsonData[targetKey]
    else:
        globals()[f'{targetKey}'] = 0
    return globals()[f'{targetKey}']


def issueNoCrl(jsonData):

    global cow
    global animalNo
    global issueNo
    global issueDate

    check = jsonData['response']['body']['items']

    if check:

        checkType = str(type(check['item']))

        if checkType == "<class 'dict'>":
            dictTargetJsonData = jsonData['response']['body']['items']['item']
            animalNo = valueExtraction('animalNo', dictTargetJsonData)
            issueNo = valueExtraction('issueNo', dictTargetJsonData)
            issueDate = valueExtraction('issueDate', dictTargetJsonData)

        elif checkType == "<class 'list'>":
            listTargetJsonData = jsonData['response']['body']['items']['item'][0]
            animalNo = valueExtraction('animalNo', listTargetJsonData)
            issueNo = valueExtraction('issueNo', listTargetJsonData)
            issueDate = valueExtraction('issueDate', listTargetJsonData)

    else:
        animalNo = cow
        issueNo = 0
        issueDate = 0


def phenoCrl(jsonData):

    global cow
    global abattDate
    global birthmonth
    global weight
    global rea
    global backfat
    global insfat
    global qgrade
    global wgrade
    global windex

    check = jsonData['response']['body']['items']

    if check:

        checkType = str(type(check['item']))

        if checkType == "<class 'dict'>":
            dictTargetJsonData = jsonData['response']['body']['items']['item']
            abattDate = valueExtraction('abattDate', dictTargetJsonData)
            birthmonth = valueExtraction('birthmonth', dictTargetJsonData)
            weight = valueExtraction('weight', dictTargetJsonData)
            rea = valueExtraction('rea', dictTargetJsonData)
            backfat = valueExtraction('backfat', dictTargetJsonData)
            insfat = valueExtraction('insfat', dictTargetJsonData)
            qgrade = valueExtraction('qgrade', dictTargetJsonData)
            wgrade = valueExtraction('wgrade', dictTargetJsonData)
            windex = valueExtraction('windex', dictTargetJsonData)

        elif checkType == "<class 'list'>":
            listTargetJsonData = jsonData['response']['body']['items']['item'][0]
            abattDate = valueExtraction('abattDate', listTargetJsonData)
            birthmonth = valueExtraction('birthmonth', listTargetJsonData)
            weight = valueExtraction('weight', listTargetJsonData)
            rea = valueExtraction('rea', listTargetJsonData)
            backfat = valueExtraction('backfat', listTargetJsonData)
            insfat = valueExtraction('insfat', listTargetJsonData)
            qgrade = valueExtraction('qgrade', listTargetJsonData)
            wgrade = valueExtraction('wgrade', listTargetJsonData)
            windex = valueExtraction('windex', listTargetJsonData)

    else:

        abattDate = 0
        birthmonth = 0
        weight = 0
        rea = 0
        backfat = 0
        insfat = 0
        qgrade = 0
        wgrade = 0
        windex = 0


def farmInfoCrl(jsonData):
    global firstFarmerNm
    global LastFarmerNm

    check = jsonData['response']['body']['items']

    if check:

        checkType = str(type(check['item']))

        if checkType == "<class 'dict'>":

            TargetJsonData = jsonData['response']['body']['items']['item']
            firstFarmerNm = valueExtraction('farmerNm', TargetJsonData)
            LastFarmerNm = valueExtraction('farmerNm', TargetJsonData)

        elif checkType == "<class 'list'>":
            TargetJsonData = jsonData['response']['body']['items']['item']
            firstListTargetJsonData = TargetJsonData[0]
            LastListTargetJsonData = TargetJsonData[len(TargetJsonData)-1]
            firstFarmerNm = valueExtraction(
                'farmerNm', firstListTargetJsonData)
            LastFarmerNm = valueExtraction('farmerNm', LastListTargetJsonData)

    else:
        firstFarmerNm = 0
        LastFarmerNm = 0
################################### test########################################


session = requestsRetrySession(3, 2, 0.2)
# key = "Y6k9yR%2FboInZk%2BHcmOIBnrg5cfuVPmdp%2BXeHidOJW4Mgd2sdwszgUoZdumUheNqTAdlcWqdzcEhGztt3p3pBjA%3D%3D"
key = "BO%2F3dIgEqyDE92lw4Uh7RJ7PudNPzn6TYr6L5dn3B98nhdBTovB4XiK4v8wjMfVf2B2zJjUQaBaC2rNC%2FQseAw%3D%3D"
# key = "TJJYF8erF9T6iz%2FqFHneEoTd8vIroczC1YXyUxQ5zVwl%2BLuoRDuLo5Muo0gdq94R%2FJH%2FQoLFf2%2FmjkEK%2BLb7Yg%3D%3D"

count = 0
cowInfoApiOption = 2
# logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
cowList = fileUpload()
crlResult = []
crlErrorList = []

for cow in cowList:
    count += 1
    crlData = []
    issueNoJsonData = issueNoRequestsConnect(session, key, cow)
    issueNoResultCode = resultCodeCheck(issueNoJsonData)
    if issueNoResultCode != '00':
        issueNoJsonData = retry(issueNoRequestsConnect(
            session, key, cow), max_tries=10)
        if errorCount < 10:
            issuNoResult = issueNoCrl(issueNoJsonData)
        else:
            crlErrorList.append(cow)
            continue
    else:
        issuNoResult = issueNoCrl(issueNoJsonData)

    if issueNo != 0:
        phenoJsonData = phenoRequestsConnect(session, key, issueNo, issueDate)
        phenoNoResultCode = resultCodeCheck(phenoJsonData)
        if phenoNoResultCode != '00':
            phenoJsonData = retry(phenoRequestsConnect(
                session, key, issueNo, issueDate), max_tries=10)
            if errorCount < 10:
                phenoResult = phenoCrl(phenoJsonData)
            else:
                crlErrorList.append(cow)
                continue
        else:
            phenoResult = phenoCrl(phenoJsonData)

        cowInfoJsonData = farmInfoRequestsConnect(
            session, key, cow, cowInfoApiOption)
        cowInfoResultCode = resultCodeCheck(cowInfoJsonData)

        if cowInfoResultCode != '00':
            cowInfoJsonData = retry(farmInfoRequestsConnect(
                session, key, cow, cowInfoApiOption), max_tries=10)
            if errorCount < 10:
                farmInfoResult = farmInfoCrl(cowInfoJsonData)
            else:
                crlErrorList.append(cow)
                continue
        else:
            farmInfoResult = farmInfoCrl(cowInfoJsonData)

    else:
        abattDate = 0
        birthmonth = 0
        weight = 0
        rea = 0
        backfat = 0
        insfat = 0
        qgrade = 0
        wgrade = 0
        windex = 0
        firstFarmerNm = 0
        LastFarmerNm = 0

    crlData.append(cow)
    crlData.append(abattDate)
    crlData.append(birthmonth)
    crlData.append(weight)
    crlData.append(rea)
    crlData.append(backfat)
    crlData.append(insfat)
    crlData.append(qgrade)
    crlData.append(wgrade)
    crlData.append(windex)
    crlData.append(firstFarmerNm)
    crlData.append(LastFarmerNm)
    crlResult.append(crlData)

    print(cow, abattDate, birthmonth, weight, rea, backfat, insfat, qgrade, wgrade,
          windex, firstFarmerNm, LastFarmerNm, f"크롤링 {len(cowList)} 중 {count}마리째 진행 중")

phenoCrlResultHeader = ["IID", 'AbattDate', "SMO", "CWT", "EMA",
                        "BFT", "MAR", "Ggrade", "Wgrade", "Windex", "FirstFarm", "LastFarm"]
phenoCrlResult_df = pd.DataFrame(
    crlResult, columns=phenoCrlResultHeader, dtype='object')
crlErrorResultHeader = ["IID"]
crlErrorResult_df = pd.DataFrame(
    crlErrorList, columns=crlErrorResultHeader, dtype='object')

fileDownload(phenoCrlResult_df, crlErrorResult_df)

print(f"결과파일 생성 완료. 에러두수:{len(crlErrorList)}")
# def phenoInfoCrl(session, key, issueNo, issueDate):

#         url  = f"http://data.ekape.or.kr/openapi-data/service/user/grade/confirm/cattle?issueNo={issueNo}&issueDate={issueDate}&ServiceKey={key}"

#         request = session.get(url)

#         xpars = xmltodict.parse(request.text)
#         jsonDump = json.dumps(xpars)
#         json_data = json.loads(jsonDump)

#         return json_data

#                                          birthmonth = valueExtraction('birthmonth', targetJsonData)
#                                         weight = valueExtraction('weight', targetJsonData)
#                                         rea = valueExtraction('rea', targetJsonData)
#                                         backfat = valueExtraction('backfat', targetJsonData)
#                                         insfat = valueExtraction('insfat', targetJsonData)
#                                         qgrade = valueExtraction('qgrade', targetJsonData)
#                                         wgrade = valueExtraction('wgrade', targetJsonData)
#                                         windex = valueExtraction('windex', targetJsonData)

#                                         print(birthmonth,weight,rea,backfat,insfat,qgrade,wgrade,windex)
