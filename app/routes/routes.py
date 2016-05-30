from app import app
from flask import render_template, request, redirect, url_for, send_file
from ..services.check_bios.main import Extractor
from app.services.check_bios.data_filter import DataFilter
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
    data_filter = DataFilter()

    regex_storing_time1 = datetime.datetime.now()
    joined_regexes, content_regexes, support_words_df, stop_words_df = data_filter.get_regexes_frames(raw_regex)
    regex_storing_time2 = datetime.datetime.now()

    bios_getting_time1 = datetime.datetime.now()
    needed_bios = data_filter.get_bios(source, source_text)
    bios_getting_time2 = datetime.datetime.now()

    extracting_time1 = datetime.datetime.now()
    extractor = Extractor(joined_regexes, content_regexes, support_words_df, stop_words_df)
    ai_result = extractor.get_ai_results(needed_bios)
    extracting_time2 = datetime.datetime.now()

    t2 = datetime.datetime.now()
    print("Regexes storing: " + str(regex_storing_time2 - regex_storing_time1))
    print("Bios_getting: " + str(bios_getting_time2 - bios_getting_time1))
    print("Extract inf0: " + str(extracting_time2 - extracting_time1))
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


