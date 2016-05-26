from app import app
from flask import render_template, request, redirect, url_for, send_file
from ..services.check_bios.main import Extractor
from ..services.check_bios.statistics import Statistics
from app.services.check_bios.data_filter import *
import os
import pandas as pd
import datetime

pd.set_option('display.max_colwidth', -1)

MAX_FILE_SIZE = 1024 * 1024 + 1
app.config['UPLOAD_FILE'] = 'app/data/full_data.csv'
ALLOWED_EXTENSIONS = set(['csv', 'xlsx'])



@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


@app.route('/checkbios', methods=['POST'])
def check_bios():
    t1 = datetime.datetime.now()
    source = request.form.get('source')
    source_text = request.form.get('source_text')

    raw_regex = request.form.get('regexes')
    joined_regexes, content_regexes = get_regexes_frames(raw_regex)

    needed_bios = get_bios(source, source_text)
    extractor = Extractor(joined_regexes, content_regexes)
    ai_result = extractor.get_ai_results(needed_bios)

    t2 = datetime.datetime.now()
    print("Time: " + str(t2 - t1))
    if not ai_result.empty:
        return render_template("result_tmp.html",
                               regex=raw_regex,
                               ai_data=ai_result.to_html(index=False))
    return redirect(url_for('index'))


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
    return send_file("static/result.xlsx",
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True, attachment_filename="result.xlsx")


@app.route('/get_no_extraction_file', methods=['GET', 'POST'])
def get_no_extractions_file():
    return send_file("static/no_extractions.xlsx",
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True,
                     attachment_filename="no_extraction_file.xlsx")


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


