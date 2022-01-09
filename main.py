import io
import os

from loguru import logger

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

SCOPES = [
  'https://www.googleapis.com/auth/drive.metadata.readonly',
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/drive',
  'https://www.googleapis.com/auth/drive.file',
]
SERVICE_ACCOUNT_FILE = 'service_account-credentials.json'
ROOT_FOLDER = os.getenv('ROOT_FOLDER', default='')

service_v2: Resource
service_v3: Resource
creds: Credentials

def init_services():
  global service_v2, service_v3, creds
  try:
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    creds.refresh(Request())
    if not creds or not creds.valid:
      print("error with credentials")
      exit(1)
    service_v2 = build('drive', 'v2', credentials=creds)
    service_v3 = build('drive', 'v3', credentials=creds)
  except Exception as err:
    print(err)
    exit(1)


def listChildren(folder=ROOT_FOLDER):
  global service_v2
  result = []
  pageToken = None
  while True:
    params = {}
    if pageToken:
      params['pageToken'] = pageToken
    results = service_v2.children().list(folderId=folder, **params).execute()
    pageToken = results.get('nextPageToken')
    items = results.get('items', [])
    for item in items:
      fid = item['id']
      obj = {'id': fid}
      file = service_v2.files().get(fileId=fid).execute()
      for k, v in file.items():
        if k in ['webContentLink', 'thumbnailLink', 'title', 'description', 'fileSize', 'fileExtension', 'mimeType']:
          obj[k] = v
          # print(k, v)
        # else:
        #   obj[k] = v
      result.append(obj)
      # print(u'{0}'.format(item['id']))
    if not pageToken:
      break
  # print("returned", result)
  return result


from flask import Flask, request, jsonify, Response, abort

init_services()
app = Flask(__name__)

@app.route('/list')
def route_list():
  folderId = request.args.get('folderId')
  if not folderId:
    folderId = ROOT_FOLDER
  try:
    result = listChildren(folderId)
    return jsonify(result)
  except HttpError as err:
    return err.status_code, err.reason
  except Exception as err:
    return 500, err

@app.route('/fetch/<string:fileId>')
def fetch(fileId):
  try:
    mimeType = service_v3.files().get(fileId=fileId, fields="mimeType, ").execute()['mimeType']
    media = service_v3.files().get_media(fileId=fileId)
    logger.info("streaming %s: %s (%d kB)" % fileId, mimeType)

    def stream():
      done = False
      buffer = io.BytesIO()
      downloader = MediaIoBaseDownload(buffer, media)
      progress = 0
      while done is False:
        _, done = downloader.next_chunk()
        buffer.seek(progress)
        yield buffer.read(downloader._progress - progress)

    return Response(stream(), mimetype=mimeType)
  except HttpError as err:
    abort(err.status_code, err.reason)
  except Exception as err:
    logger.error(err)
    abort(500, err)

if __name__ == '__main__':
  app.run()
