from app import app
from flask import Response, render_template, request, redirect, url_for, send_from_directory
from ..services.check_bios.main import *
from werkzeug.utils import secure_filename
import os
from io import StringIO

# import csv

MAX_FILE_SIZE = 1024 * 1024 + 1
app.config['UPLOAD_FILE'] = 'app/models/full_data.xlsx'
ALLOWED_EXTENSIONS = set(['csv', 'xlsx'])


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


@app.route('/checkbios', methods=['POST'])
def check_bios():
    source = request.form.get('source')
    source_text = request.form.get('source_text')
    raw_regex = request.form.get('regexes')
    bios = get_bios(source, source_text)
    regexes = get_regexes(raw_regex)
    if bios and raw_regex:
        table = predict(bios, regexes)
        return render_template("result.html", data=table.to_html())
    return redirect(url_for('index'))


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


@app.route('/file_upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename):
            file.save(os.path.join(app.config['UPLOAD_FILE']))
            return redirect(url_for('index'))
    return render_template("test.html")


@app.route('/get_result_file', methods=['GET', 'POST'])
def get_result_file():
    return send_from_directory(app.static_folder, "test.csv")
