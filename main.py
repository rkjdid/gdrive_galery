import io
import os
import time

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
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_JSON', default='service_account-credentials.json')
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
      logger.error("error with credentials")
      exit(1)
    service_v2 = build('drive', 'v2', credentials=creds)
    service_v3 = build('drive', 'v3', credentials=creds)
  except Exception as err:
    logger.error(err)
    exit(1)


def listChildren(folder=ROOT_FOLDER):
  global service_v2
  result = []
  pageToken = None
  while True:
    params = {}
    if pageToken:
      params['pageToken'] = pageToken
    params['fields'] = "nextPageToken, files(id, name)"
    results = service_v3.files().list(q="'%s' in parents" % folder, **params).execute()
    pageToken = results.get('nextPageToken')
    items = results.get('files', [])
    for item in items:
      fid = item['id']
      f = service_v3.files().get(fileId=fid, fields="webContentLink, thumbnailLink, description, size, fileExtension, mimeType").execute()
      f['fetchEndpoint'] = '/fetch/%s' % fid
      f['size'] = "%.1f kB" % (float(f.get('size', 0.)) / 1024.)
      # logger.info(f)
      result.append(f)
    if not pageToken:
      break
  return result


from flask import Flask, request, jsonify, Response, abort

init_services()
app = Flask(__name__)

@app.route('/list', defaults={'folderId': ROOT_FOLDER})
@app.route('/list/', defaults={'folderId': ROOT_FOLDER})
@app.route('/list/<string:folderId>')
def route_list(folderId):
  try:
    result = listChildren(folderId)
    return jsonify(result)
  except HttpError as err:
    return abort(err.status_code, err.reason)
  except Exception as err:
    logger.exception(err)
    return abort(500, err)

@app.route('/fetch/<string:fileId>')
def fetch(fileId):
  try:
    meta = service_v3.files().get(fileId=fileId, fields="mimeType, size").execute()
    t0 = time.time()
    logger.info("streaming %s: %s (%f kB)" % (fileId, meta.get('mimeType'), float(meta.get('size', 0.)) / 1024.))
    media = service_v3.files().get_media(fileId=fileId)
    def stream():
      done = False
      buffer = io.BytesIO()
      downloader = MediaIoBaseDownload(buffer, media)
      progress = 0
      while done is False:
        _, done = downloader.next_chunk()
        buffer.seek(progress)
        yield buffer.read(downloader._progress - progress)
      logger.info("done in %.1fs" % (time.time() - t0))
    return Response(stream(), mimetype=meta.get('mimeType'))
  except HttpError as err:
    abort(err.status_code, err.reason)
  except Exception as err:
    logger.exception(err)
    abort(500, err)

if __name__ == '__main__':
  app.run()

application = app
