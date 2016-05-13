from app import app
from flask import Response, render_template, request, redirect, url_for,  send_file
from ..services.check_bios.main import BiosExtractor
from ..services.check_bios.statistics import Statistics
from ..services.check_bios.pr_areas_spec_handler import *
import os
import multiprocessing
import pandas as pd
pd.set_option('display.max_colwidth', -1)

# import csv

MAX_FILE_SIZE = 1024 * 1024 + 1
app.config['UPLOAD_FILE'] = 'app/models/full_data.csv'
ALLOWED_EXTENSIONS = set(['csv', 'xlsx'])


@app.route('/lead_up', methods=['GET', 'POST'])
def lead_up():
    return render_template('lead-up.html', ldb_data=get_all_specialities())


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')




@app.route('/checkbios', methods=['POST'])
def check_bios():
    source = request.form.get('source')
    source_text = request.form.get('source_text')
    raw_regex = request.form.get('regexes')
    specialities_regex_filter = request.form.get('spec_regex')

    needed_bios = get_bios(source, source_text)
    regexes = get_regexes(raw_regex)
    ldb_result = get_bios_per_spec(specialities_regex_filter)

    ai_result = DataFrame()
    if needed_bios and raw_regex:
        predictor = BiosExtractor(regexes)
        pool = multiprocessing.Pool(1)
        table = pool.map(predictor.predict, needed_bios)
        ai_result = pd.concat(table, ignore_index=True)

    equals = len(Statistics.get_equals(ai_result, ldb_result, "profileUrl"))
    ai_only = len(Statistics.get_differs(ai_result, ldb_result, "profileUrl"))
    ldb_only = Statistics.get_differs(ldb_result, ai_result, "profileUrl")
    ldb_only_table = DataFrame()
    if ldb_only:
        ldb_only_table = ldb_result[ldb_result['profileUrl'].isin(ldb_only)]

    if not ai_result.empty or not ldb_result.empty:
        return render_template("result.html", ai_data=ai_result.to_html(), ldb_data=ldb_result.to_html(),
                               equals=equals, ai_only=ai_only, ldb_only=ldb_only,
                               ldb_only_table=ldb_only_table.to_html())
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
