import os
import flask
import boto.s3.connection
import cx_Oracle

class DB:

    def __enter__(self):
        self.db = cx_Oracle.connect(os.environ['DB_USER'], 
                                    os.environ['DB_PASSWORD'], 
                                    db_dsn)
        self.c = self.db.cursor()
        return self.c

    def __exit__(self, exc_type, exc_value, exc_traceback):
        try:
            self.c.close()
            self.db.close()
        except:
            pass
        return

class Summary:

    def __init__(self):
        with DB() as c:
            c.execute("SELECT * FROM summary")
            c.arraysize = 1000
            cols = [ el[0] for el in c.description ]
            self.rows = [ dict(zip(cols, row)) for row in c ]
        return

class Volume:

    def __init__(self, s3_link):
        self.s3_link = s3_link
        query = "SELECT * FROM image03 WHERE image_file = :image_file"
        query_params = {'image_file': s3_link}
        (cols, rows) = self._db(query, query_params)
        if not rows:
            raise KeyError('%s not found' % s3_link)
        self.image03_cols = cols
        self.image03 = [ dict(zip(cols, row)) for row in rows ]
        query = "SELECT * FROM image03_derived WHERE image_file = :image_file"
        (cols, rows) = self._db(query, query_params)
        if not rows:
            self.image03_derived = None
            self.thumbnail_link = None
        else:
            self.image03_derived = dict(zip(cols, rows[0]))
            if self.image03_derived['image_thumbnail_file']:
                itf = self.image03_derived['image_thumbnail_file']
                self.thumbnail_link = '/thumbnail/%s' % itf
        return

    def _db(self, query, query_params):
        with DB() as c:
            c.execute(query, query_params)
            cols = [ el[0].lower() for el in c.description ]
            rows = c.fetchall()
        return (cols, rows)

db_dsn = cx_Oracle.makedsn(os.environ['DB_HOST'], 
                           1521, 
                           os.environ['DB_SERVICE'])

app = flask.Flask(__name__)

summary = Summary()

@app.route('/')
def index():
    return flask.render_template('index.tmpl', 
                                 unchecked=unchecked, 
                                 multiples=multiples, 
                                 not_mri=not_mri)

@app.route('/volume/<path:spec>')
def volume(spec):
    try:
        volume = Volume(spec)
    except KeyError:
        flask.abort(404)
    return flask.render_template('volume.tmpl', volume=volume)

@app.route('/thumbnail/<path:spec>')
def thumbnail(spec):
    if not spec.startswith('s3://'):
        flask.abort(404)
    bucket_name = spec[5:].split('/')[0]
    key = spec[5+len(bucket_name)+1:]
    cf = boto.s3.connection.OrdinaryCallingFormat()
    s3 = boto.s3.connection.S3Connection(os.environ['AWS_ACCESS_KEY_ID'], 
                                         os.environ['AWS_SECRET_ACCESS_KEY'], 
                                         calling_format=cf)
    bucket = s3.get_bucket(bucket_name)
    k = bucket.get_key(key)
    if k is None:
        flask.abort(404)
    resp = flask.Response(k.get_contents_as_string(), 
                          status=200, 
                          mimetype='image/png')
    return resp

print 'ready'

app.run(debug=True)

# eof
